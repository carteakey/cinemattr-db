"""Extract structured filters + semantic query from a natural-language movie search.

Single LLM call returns JSON. Replaces LangChain SelfQueryRetriever.
"""
from __future__ import annotations

import json
import logging
import os
import re
from dataclasses import dataclass, field
from typing import Any

from openai import OpenAI

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ParsedQuery:
    semantic_query: str
    year_from: int | None = None
    year_to: int | None = None
    genres: tuple[str, ...] = field(default_factory=tuple)
    min_rating: float | None = None

    def has_filter(self) -> bool:
        return any(
            value is not None and value != ()
            for value in (self.year_from, self.year_to, self.genres or None, self.min_rating)
        )


SYSTEM_PROMPT = """You extract structured filters from a movie search query.

Return ONLY a JSON object with these keys:
- "semantic_query": the core plot/mood/theme description, stripped of filters (string, required)
- "year_from": inclusive lower bound year (integer or null)
- "year_to": inclusive upper bound year (integer or null)
- "genres": list of lowercase genre names like "thriller", "comedy" (array, may be empty)
- "min_rating": minimum IMDb rating 0-10 (number or null)

If the user says "after 2010", set year_from=2010. If "90s", year_from=1990, year_to=1999.
If no filter mentioned, set to null / empty array.
Do not invent filters the user didn't state."""


@dataclass(frozen=True)
class LLMProvider:
    name: str
    client: OpenAI
    model: str


def _build_providers() -> list[LLMProvider]:
    providers: list[LLMProvider] = []
    local_base_url = (os.getenv("LOCAL_LLM_BASE_URL") or "").strip()
    local_model = (os.getenv("LOCAL_LLM_MODEL") or "").strip()
    if local_base_url and local_model:
        providers.append(
            LLMProvider(
                name="local",
                client=OpenAI(
                    api_key=(os.getenv("LOCAL_LLM_API_KEY") or "local").strip(),
                    base_url=local_base_url,
                    timeout=float(os.getenv("LOCAL_LLM_TIMEOUT_SECONDS", "45")),
                    max_retries=0,
                ),
                model=local_model,
            )
        )

    cloud_base_url = (os.getenv("LLM_BASE_URL") or "").strip()
    cloud_api_key = (os.getenv("LLM_API_KEY") or os.getenv("OPENAI_API_KEY") or "").strip()
    if cloud_base_url and cloud_api_key:
        providers.append(
            LLMProvider(
                name="cloud",
                client=OpenAI(api_key=cloud_api_key, base_url=cloud_base_url),
                model=os.getenv("LLM_MODEL", "gpt-4.1-mini").strip(),
            )
        )

    if not providers:
        raise RuntimeError("Configure LOCAL_LLM_* or LLM_* for query parsing.")
    return providers


def _extract_json(text: str) -> dict[str, Any]:
    match = re.search(r"\{.*\}", text, re.DOTALL)
    if not match:
        raise ValueError("No JSON object in model response.")
    return json.loads(match.group(0))


def parse_query(raw_query: str) -> ParsedQuery:
    payload = ""
    last_error: Exception | None = None
    for provider in _build_providers():
        try:
            response = provider.client.chat.completions.create(
                model=provider.model,
                temperature=0,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": raw_query},
                ],
                response_format={"type": "json_object"},
            )
            payload = response.choices[0].message.content or ""
            logger.info("Query parsed with %s provider (%s).", provider.name, provider.model)
            break
        except Exception as exc:  # provider failure should allow the next configured provider
            last_error = exc
            logger.warning(
                "%s query parser unavailable (%s); trying fallback.",
                provider.name,
                type(exc).__name__,
            )
    else:
        assert last_error is not None
        raise last_error

    try:
        data = _extract_json(payload)
    except (ValueError, json.JSONDecodeError) as exc:
        logger.warning("Filter parse failed (%s); falling back to raw query.", exc)
        return ParsedQuery(semantic_query=raw_query)

    semantic_query = str(data.get("semantic_query") or raw_query).strip() or raw_query
    year_from = data.get("year_from")
    year_to = data.get("year_to")
    min_rating = data.get("min_rating")
    genres_raw = data.get("genres") or []

    return ParsedQuery(
        semantic_query=semantic_query,
        year_from=int(year_from) if isinstance(year_from, (int, float)) else None,
        year_to=int(year_to) if isinstance(year_to, (int, float)) else None,
        genres=tuple(str(g).strip().lower() for g in genres_raw if str(g).strip()),
        min_rating=float(min_rating) if isinstance(min_rating, (int, float)) else None,
    )
