"""Thin OpenAI-compatible embedding client.

Works with OpenAI, Gemini's OpenAI-compat endpoint, local providers, etc.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Iterable, Sequence

from openai import OpenAI


@dataclass(frozen=True)
class EmbeddingConfig:
    base_url: str
    api_key: str
    model: str
    dim: int | None


def load_embedding_config() -> EmbeddingConfig:
    base_url = (os.getenv("EMBEDDING_BASE_URL") or os.getenv("LLM_BASE_URL") or "").strip()
    api_key = (os.getenv("EMBEDDING_API_KEY") or os.getenv("LLM_API_KEY") or os.getenv("OPENAI_API_KEY") or "").strip()
    model = os.getenv("EMBEDDING_MODEL", "text-embedding-3-small").strip()

    if not base_url:
        raise RuntimeError("EMBEDDING_BASE_URL or LLM_BASE_URL is required.")
    if not api_key:
        raise RuntimeError("EMBEDDING_API_KEY or LLM_API_KEY is required.")

    raw_dim = os.getenv("EMBEDDING_DIM", "").strip()
    dim = int(raw_dim) if raw_dim else None

    return EmbeddingConfig(base_url=base_url, api_key=api_key, model=model, dim=dim)


def build_client(config: EmbeddingConfig) -> OpenAI:
    return OpenAI(api_key=config.api_key, base_url=config.base_url)


def embed_texts(client: OpenAI, config: EmbeddingConfig, texts: Sequence[str]) -> list[list[float]]:
    kwargs: dict = {"model": config.model, "input": list(texts)}
    if config.dim is not None:
        kwargs["dimensions"] = config.dim
    response = client.embeddings.create(**kwargs)
    return [item.embedding for item in response.data]


def embed_one(client: OpenAI, config: EmbeddingConfig, text: str) -> list[float]:
    return embed_texts(client, config, [text])[0]


def iter_batches(items: Iterable[str], batch_size: int) -> Iterable[list[str]]:
    batch: list[str] = []
    for item in items:
        batch.append(item)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch
