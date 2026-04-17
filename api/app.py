"""Cinemattr search API (FastAPI + DuckDB VSS, no LangChain)."""
from __future__ import annotations

import logging
import os
import time
from collections import defaultdict, deque
from functools import lru_cache
from threading import Lock
from typing import Deque
from urllib.parse import urlparse

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from embeddings import build_client, embed_one, load_embedding_config
from filter_parser import parse_query
from vector_store import connect, load_store_config, search

logger = logging.getLogger("cinemattr.api")
logging.basicConfig(level=logging.INFO)


def _parse_int_env(name: str, default: int, minimum: int = 1, maximum: int = 1000) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        parsed = int(raw)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be an integer.") from exc
    if parsed < minimum or parsed > maximum:
        raise RuntimeError(f"{name} must be between {minimum} and {maximum}.")
    return parsed


def _parse_bool_env(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    normalized = raw.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise RuntimeError(f"{name} must be a boolean.")


RATE_LIMIT_REQUESTS = _parse_int_env("RATE_LIMIT_REQUESTS", 30, maximum=10_000)
RATE_LIMIT_WINDOW_SECONDS = _parse_int_env("RATE_LIMIT_WINDOW_SECONDS", 60, maximum=3_600)
TRUST_PROXY_HEADERS = _parse_bool_env("TRUST_PROXY_HEADERS", False)
TRUSTED_PROXY_IPS = {
    ip.strip() for ip in os.getenv("TRUSTED_PROXY_IPS", "").split(",") if ip.strip()
}
ALLOWED_ORIGINS = [
    origin.strip() for origin in os.getenv("ALLOWED_ORIGINS", "*").split(",") if origin.strip()
] or ["*"]

DEFAULT_TOP_K = _parse_int_env("VECTOR_TOP_K", 20, maximum=200)

_rate_state: dict[str, Deque[float]] = defaultdict(deque)
_rate_lock = Lock()


def _enforce_rate_limit(client_ip: str) -> None:
    now = time.monotonic()
    window_start = now - RATE_LIMIT_WINDOW_SECONDS
    with _rate_lock:
        timestamps = _rate_state[client_ip]
        while timestamps and timestamps[0] < window_start:
            timestamps.popleft()
        if len(timestamps) >= RATE_LIMIT_REQUESTS:
            raise HTTPException(status_code=429, detail="Rate limit exceeded. Slow down.")
        timestamps.append(now)


def _resolve_client_ip(request: Request) -> str:
    if TRUST_PROXY_HEADERS:
        forwarded = request.headers.get("x-forwarded-for", "")
        if forwarded:
            first = forwarded.split(",")[0].strip()
            if first:
                return first
    client = request.client
    if client and client.host:
        if TRUST_PROXY_HEADERS and TRUSTED_PROXY_IPS and client.host not in TRUSTED_PROXY_IPS:
            # caller claims proxy but isn't trusted; use socket peer
            return client.host
        return client.host
    return "unknown"


def _normalize_query(raw: str) -> str:
    return " ".join(raw.split()).strip()


def _resolve_top_k(requested: int | None) -> int:
    if requested is None:
        return DEFAULT_TOP_K
    return max(1, min(requested, 200))


class SearchRequest(BaseModel):
    query: str = Field(..., min_length=2, max_length=500)
    top_k: int | None = Field(default=None, ge=1, le=200)


@lru_cache(maxsize=1)
def _shared_resources():
    store_config = load_store_config()
    embedding_config = load_embedding_config()
    embedding_client = build_client(embedding_config)
    return store_config, embedding_config, embedding_client


def _get_connection():
    store_config, _, _ = _shared_resources()
    return connect(store_config, read_only=True)


def _run_search(query: str, top_k: int) -> dict:
    store_config, embedding_config, embedding_client = _shared_resources()
    parsed = parse_query(query)
    vector = embed_one(embedding_client, embedding_config, parsed.semantic_query)
    con = connect(store_config, read_only=True)
    try:
        titles = search(con, store_config, vector, parsed, top_k)
    finally:
        con.close()
    return {
        "titles": titles,
        "query": query,
        "semantic_query": parsed.semantic_query,
        "filters": {
            "year_from": parsed.year_from,
            "year_to": parsed.year_to,
            "genres": list(parsed.genres),
            "min_rating": parsed.min_rating,
        },
    }


app = FastAPI(title="cinemattr search api", version="3.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
    allow_credentials=False,
)


@app.exception_handler(HTTPException)
async def _http_exception_handler(request: Request, exc: HTTPException) -> JSONResponse:
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": {"message": str(exc.detail), "status": exc.status_code}},
    )


@app.exception_handler(RequestValidationError)
async def _validation_exception_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
    return JSONResponse(
        status_code=422,
        content={"error": {"message": "Invalid request payload.", "status": 422, "details": exc.errors()}},
    )


@app.exception_handler(Exception)
async def _unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    logger.exception("Unhandled error during %s %s", request.method, request.url.path)
    return JSONResponse(
        status_code=500,
        content={"error": {"message": str(exc), "status": 500}},
    )


@app.get("/health")
def health() -> dict[str, str | int | bool]:
    store_config = None
    vss_available = False
    row_count = -1
    try:
        store_config = load_store_config()
        con = connect(store_config, read_only=True)
        try:
            row_count = con.execute(f"SELECT COUNT(*) FROM {store_config.table};").fetchone()[0]
            vss_available = True
        finally:
            con.close()
    except Exception as exc:  # noqa: BLE001 — surfaced in health payload
        logger.warning("Health check: DuckDB not ready: %s", exc)

    return {
        "status": "ok",
        "service": "cinemattr-search-api",
        "version": "3.0.0",
        "model": os.getenv("LLM_MODEL", "gpt-4.1-mini"),
        "embedding_model": os.getenv("EMBEDDING_MODEL", "text-embedding-3-small"),
        "db_path": store_config.db_path if store_config else "unset",
        "table": store_config.table if store_config else "unset",
        "vss_available": vss_available,
        "indexed_rows": row_count,
    }


@app.post("/search")
def search_post(payload: SearchRequest, request: Request) -> dict:
    _enforce_rate_limit(_resolve_client_ip(request))
    query = _normalize_query(payload.query)
    if not query:
        raise HTTPException(status_code=422, detail="Query is empty after normalization.")
    return _run_search(query, _resolve_top_k(payload.top_k))


@app.get("/search")
def search_get(
    request: Request,
    query: str = Query(..., min_length=2, max_length=500),
    top_k: int | None = Query(default=None, ge=1, le=200),
) -> dict:
    _enforce_rate_limit(_resolve_client_ip(request))
    normalized = _normalize_query(query)
    if not normalized:
        raise HTTPException(status_code=422, detail="Query is empty after normalization.")
    return _run_search(normalized, _resolve_top_k(top_k))
