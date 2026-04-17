import logging
import os
import re
import time
from collections import defaultdict, deque
from functools import lru_cache
from threading import Lock
from typing import Any, Deque
from urllib.parse import urlparse

import langchain
import pinecone
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from langchain.cache import SQLiteCache
from langchain.chains.query_constructor.base import AttributeInfo
from langchain.chat_models import ChatOpenAI
from langchain.embeddings import OpenAIEmbeddings
from langchain.retrievers.self_query.base import SelfQueryRetriever
from langchain.vectorstores import Pinecone
from pydantic import BaseModel, Field

logger = logging.getLogger("cinemattr.api")
logging.basicConfig(level=logging.INFO)

langchain.llm_cache = SQLiteCache(
    database_path=os.getenv("LANGCHAIN_CACHE_PATH", "/tmp/langchain.db")
)


def _parse_int_env(name: str, default: int, minimum: int = 1, maximum: int = 1000) -> int:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default

    try:
        parsed = int(raw_value)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be an integer.") from exc

    if parsed < minimum or parsed > maximum:
        raise RuntimeError(f"{name} must be between {minimum} and {maximum}.")

    return parsed


def _parse_bool_env(name: str, default: bool = False) -> bool:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default

    normalized = raw_value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False

    raise RuntimeError(f"{name} must be a boolean.")


def _normalize_query(raw_query: str) -> str:
    query = raw_query.lower().strip()
    query = re.sub(r"[^0-9A-Za-z .-]", "", query)
    query = re.sub(r"\s+", " ", query).strip()

    if query.endswith("."):
        query = query[:-1].strip()

    return query


def _is_local_base_url(base_url: str) -> bool:
    if not base_url:
        return False

    parsed = urlparse(base_url)
    hostname = (parsed.hostname or "").lower()
    return hostname in {"localhost", "127.0.0.1", "0.0.0.0", "::1"} or hostname.endswith(
        ".local"
    )


def _resolve_api_key(primary_env: str, fallback_envs: tuple[str, ...], base_url: str) -> str:
    primary = os.getenv(primary_env, "").strip()
    if primary:
        return primary

    for fallback_env in fallback_envs:
        fallback = os.getenv(fallback_env, "").strip()
        if fallback:
            return fallback

    if "openai.com" in base_url.lower():
        raise RuntimeError(
            f"Missing API key. Set {primary_env} (or one of {', '.join(fallback_envs)}) for OpenAI endpoints."
        )

    if _is_local_base_url(base_url):
        return "not-needed"

    raise RuntimeError(
        f"Missing API key. Set {primary_env} (or one of {', '.join(fallback_envs)}) unless the provider is local."
    )


def _resolve_index_name() -> str:
    index_name = os.getenv("VECTOR_INDEX_NAME", "").strip()
    if index_name:
        return index_name

    legacy_index_name = os.getenv("PINECONE_INDEX_NAME", "").strip()
    if legacy_index_name:
        return legacy_index_name

    raise RuntimeError("Missing index name. Set VECTOR_INDEX_NAME or PINECONE_INDEX_NAME.")


def _resolve_llm_base_url() -> str:
    return os.getenv("LLM_BASE_URL", "").strip()


def _resolve_embedding_base_url() -> str:
    return os.getenv("EMBEDDING_BASE_URL", "").strip() or _resolve_llm_base_url()


def _resolve_embedding_provider() -> str:
    provider = os.getenv("EMBEDDING_PROVIDER", "openai-compatible").strip().lower()
    provider_aliases = {
        "openai": "openai-compatible",
        "openai-compatible": "openai-compatible",
        "huggingface": "huggingface",
        "hf": "huggingface",
    }
    if provider not in provider_aliases:
        raise RuntimeError(
            "Unsupported EMBEDDING_PROVIDER. Use 'openai-compatible' or 'huggingface'."
        )

    return provider_aliases[provider]


def _resolve_top_k(request_top_k: int | None) -> int:
    if request_top_k is not None:
        return request_top_k

    return _parse_int_env("VECTOR_TOP_K", default=20, minimum=1, maximum=50)


TRUST_PROXY_HEADERS = _parse_bool_env("TRUST_PROXY_HEADERS", default=False)
TRUSTED_PROXY_IPS = {
    value.strip()
    for value in os.getenv("TRUSTED_PROXY_IPS", "").split(",")
    if value.strip()
}


def _resolve_client_ip(request: Request) -> str:
    client_host = request.client.host if request.client and request.client.host else ""
    if TRUST_PROXY_HEADERS:
        if TRUSTED_PROXY_IPS and client_host not in TRUSTED_PROXY_IPS:
            return client_host or "unknown"

        forwarded_for = request.headers.get("x-forwarded-for", "")
        if forwarded_for:
            return forwarded_for.split(",")[0].strip()

    if client_host:
        return client_host

    return "unknown"


RATE_LIMIT_REQUESTS = _parse_int_env("RATE_LIMIT_REQUESTS", default=30, minimum=1, maximum=1000)
RATE_LIMIT_WINDOW_SECONDS = _parse_int_env(
    "RATE_LIMIT_WINDOW_SECONDS", default=60, minimum=1, maximum=3600
)
RATE_LIMIT_BUCKETS: dict[str, Deque[float]] = defaultdict(deque)
RATE_LIMIT_LOCK = Lock()


def _enforce_rate_limit(client_ip: str) -> None:
    now = time.time()
    cutoff = now - RATE_LIMIT_WINDOW_SECONDS

    with RATE_LIMIT_LOCK:
        bucket = RATE_LIMIT_BUCKETS[client_ip]
        while bucket and bucket[0] < cutoff:
            bucket.popleft()

        if len(bucket) >= RATE_LIMIT_REQUESTS:
            raise HTTPException(
                status_code=429,
                detail=(
                    "Rate limit exceeded. Reduce request frequency and retry in a moment."
                ),
            )

        bucket.append(now)


metadata_field_info = [
    AttributeInfo(
        name="title",
        description="The title of the movie (in lowercase). Case sensitive",
        type="string",
    ),
    AttributeInfo(
        name="genre",
        description="The genres of the movie (in lowercase). Case sensitive",
        type="string or list[string]",
    ),
    AttributeInfo(
        name="year",
        description="The year the movie was released. Only integers allowed",
        type="integer",
    ),
    AttributeInfo(
        name="stars",
        description="The name of the movie actors (in lowercase). Case sensitive",
        type="string or list[string]",
    ),
    AttributeInfo(
        name="directors",
        description="The name of the movie directors (in lowercase). Case sensitive",
        type="string or list[string]",
    ),
    AttributeInfo(
        name="runtime",
        description="The runtime of the movie in minutes",
        type="string",
    ),
    AttributeInfo(
        name="imdb_rating",
        description="A 1-10 rating for the movie on IMDB",
        type="float",
    ),
    AttributeInfo(
        name="ratingCount",
        description="How many people rated the movie on IMDB. Indicator of movie's popularity",
        type="integer",
    ),
]
document_content_description = "Summary and plot of the movie"


@lru_cache(maxsize=1)
def _get_embeddings() -> Any:
    embedding_provider = _resolve_embedding_provider()
    if embedding_provider == "huggingface":
        try:
            from langchain.embeddings import HuggingFaceEmbeddings
        except ImportError as exc:
            raise RuntimeError(
                "Hugging Face embeddings require the sentence-transformers package."
            ) from exc

        embedding_model = os.getenv(
            "EMBEDDING_MODEL", "sentence-transformers/all-mpnet-base-v2"
        ).strip()
        embedding_device = os.getenv("HF_EMBEDDING_DEVICE", "").strip()
        model_kwargs = {"device": embedding_device} if embedding_device else {}

        return HuggingFaceEmbeddings(
            model_name=embedding_model,
            model_kwargs=model_kwargs,
        )

    embedding_base_url = _resolve_embedding_base_url()
    embedding_api_key = _resolve_api_key(
        "EMBEDDING_API_KEY", ("LLM_API_KEY", "OPENAI_API_KEY"), embedding_base_url
    )
    embedding_model = os.getenv("EMBEDDING_MODEL", "text-embedding-3-small").strip()

    embeddings_kwargs = {
        "model": embedding_model,
        "openai_api_key": embedding_api_key,
    }

    if embedding_base_url:
        embeddings_kwargs["openai_api_base"] = embedding_base_url

    return OpenAIEmbeddings(**embeddings_kwargs)


@lru_cache(maxsize=1)
def _get_llm() -> ChatOpenAI:
    llm_base_url = _resolve_llm_base_url()
    llm_api_key = _resolve_api_key("LLM_API_KEY", ("OPENAI_API_KEY",), llm_base_url)
    llm_model = os.getenv("LLM_MODEL", "gpt-4.1-mini").strip()

    llm_kwargs = {
        "temperature": 0,
        "model_name": llm_model,
        "openai_api_key": llm_api_key,
    }

    if llm_base_url:
        llm_kwargs["openai_api_base"] = llm_base_url

    return ChatOpenAI(**llm_kwargs)


@lru_cache(maxsize=1)
def _get_vectorstore() -> Pinecone:
    pinecone_api_key = os.getenv("PINECONE_API_KEY", "").strip()
    pinecone_environment = os.getenv("PINECONE_ENV", "").strip()

    if not pinecone_api_key:
        raise RuntimeError("Missing PINECONE_API_KEY.")
    if not pinecone_environment:
        raise RuntimeError("Missing PINECONE_ENV.")

    pinecone.init(api_key=pinecone_api_key, environment=pinecone_environment)

    return Pinecone.from_existing_index(_resolve_index_name(), _get_embeddings())


def _get_results(input_query: str, top_k: int) -> list[str]:
    retriever = SelfQueryRetriever.from_llm(
        _get_llm(),
        _get_vectorstore(),
        document_content_description,
        metadata_field_info,
        verbose=False,
    )
    retriever.search_kwargs = {"k": top_k}
    results = retriever.get_relevant_documents(input_query)

    titles = [result.metadata["source"] for result in results if "source" in result.metadata]
    return list(dict.fromkeys(titles))


class SearchRequest(BaseModel):
    query: str = Field(..., min_length=2, max_length=500)
    top_k: int | None = Field(default=None, ge=1, le=50)


app = FastAPI(title="cinemattr search api", version="2.0.0")

allowed_origins = [
    origin.strip()
    for origin in os.getenv("ALLOWED_ORIGINS", "*").split(",")
    if origin.strip()
]
if not allowed_origins:
    allowed_origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


@app.exception_handler(HTTPException)
async def _http_exception_handler(_: Request, exc: HTTPException) -> JSONResponse:
    message = exc.detail if isinstance(exc.detail, str) else "Request failed."
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": {"message": message, "status": exc.status_code}},
    )


@app.exception_handler(RequestValidationError)
async def _validation_exception_handler(
    _: Request, exc: RequestValidationError
) -> JSONResponse:
    return JSONResponse(
        status_code=422,
        content={
            "error": {
                "message": "Request validation failed.",
                "status": 422,
                "details": exc.errors(),
            }
        },
    )


@app.exception_handler(Exception)
async def _unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    logger.exception("Unhandled error during %s %s", request.method, request.url.path)
    return JSONResponse(
        status_code=500,
        content={"error": {"message": str(exc), "status": 500}},
    )


@app.get("/health")
def health() -> dict[str, str]:
    configured_index = os.getenv("VECTOR_INDEX_NAME", "").strip() or os.getenv(
        "PINECONE_INDEX_NAME", ""
    ).strip()
    return {
        "status": "ok",
        "service": "cinemattr-search-api",
        "index": configured_index or "unset",
        "model": os.getenv("LLM_MODEL", "gpt-4.1-mini"),
        "embedding_provider": _resolve_embedding_provider(),
    }


@app.post("/search")
def search(payload: SearchRequest, request: Request) -> dict[str, list[str] | str]:
    _enforce_rate_limit(_resolve_client_ip(request))
    query = _normalize_query(payload.query)

    if not query:
        raise HTTPException(status_code=422, detail="Query is empty after normalization.")

    titles = _get_results(query, _resolve_top_k(payload.top_k))
    return {"titles": titles, "query": query}


@app.get("/search")
def search_get(
    request: Request,
    query: str = Query(..., min_length=2, max_length=500),
    top_k: int | None = Query(default=None, ge=1, le=50),
) -> dict[str, list[str] | str]:
    _enforce_rate_limit(_resolve_client_ip(request))
    normalized = _normalize_query(query)

    if not normalized:
        raise HTTPException(status_code=422, detail="Query is empty after normalization.")

    titles = _get_results(normalized, _resolve_top_k(top_k))
    return {"titles": titles, "query": normalized}
