# cinemattr-db

Backend and database of [cinemattr.ca](https://github.com/carteakey/cinemattr.ca).

## How it works

- Natural-language plot search over a local movie corpus.
- One LLM call (OpenAI-compatible — OpenAI, Gemini, local) parses the query into a structured filter (year range, genres, min rating) plus a semantic query.
- Semantic query is embedded and matched against a **DuckDB VSS** HNSW index.
- Metadata filters (year / genre / rating) are applied in the same SQL query.
- Returns the top-k IMDb title IDs.
- API is a standalone FastAPI service. No LangChain. No Pinecone.

## Search API

### Routes

- `GET /health`
- `GET /ready` (returns 503 until the vector store contains rows)
- `POST /search` with JSON body: `{ "query": "...", "top_k": 20 }`
- `GET /search?query=...&top_k=20`

### Example

```bash
curl -X POST "http://localhost:8080/search" \
  -H "Content-Type: application/json" \
  -d '{"query":"mind-bending thriller with dream layers, after 2010"}'
```

## Environment variables

```bash
# Optional local-first LLM (filter parser)
LOCAL_LLM_BASE_URL=http://kpc-cachy.llama-alnair.ts.net:8001/v1
LOCAL_LLM_API_KEY=...
LOCAL_LLM_MODEL=gemma-4-26b-qat-mtp
LOCAL_LLM_TIMEOUT_SECONDS=45

# Cloud fallback
LLM_BASE_URL=https://api.openai.com/v1
LLM_API_KEY=...
LLM_MODEL=gpt-4.1-mini

# Embeddings remain separately configurable (falls back to LLM_* if unset)
EMBEDDING_BASE_URL=
EMBEDDING_API_KEY=
EMBEDDING_MODEL=text-embedding-3-small
EMBEDDING_DIM=1536          # must match the model's output dim
EMBEDDING_QUERY_PREFIX=
EMBEDDING_DOCUMENT_PREFIX=

# Vector store
DUCKDB_PATH=data/cinemattr.duckdb
VSS_TABLE=movie_embeddings
VECTOR_TOP_K=20

# API hardening
RATE_LIMIT_REQUESTS=30
RATE_LIMIT_WINDOW_SECONDS=60
ALLOWED_ORIGINS=*
TRUST_PROXY_HEADERS=false
TRUSTED_PROXY_IPS=
```

### Embedding dim cheat sheet

| Model | Default dim | Matryoshka options |
|---|---|---|
| `text-embedding-3-small` | 1536 | fixed |
| `text-embedding-3-large` | 3072 | 256..3072 |
| `gemini-embedding-001` | 3072 | 768 / 1536 / 3072 |
| `nomic-embed-text-v1.5` | 768 | 64 / 128 / 256 / 512 / 768 |

Set `EMBEDDING_DIM` to match. Changing dim requires reloading the VSS table.
For Nomic, set `EMBEDDING_QUERY_PREFIX=search_query: ` and
`EMBEDDING_DOCUMENT_PREFIX=search_document: `; the trailing spaces matter.

## Loading the vector store

Assumes you already have a DuckDB database populated by the data pipeline below.

```bash
python api/load_embeddings.py \
  --source db/duckdb/movies.duckdb \
  --table movies \
  --id-col title_id \
  --plot-col plot \
  --title-col title \
  --year-col year \
  --genre-col genres \
  --rating-col imdb_rating \
  --batch 64
```

Idempotent — re-runs upsert by `title_id`.
The loader uses Arrow batches and creates the HNSW index after ingestion; avoid
exposing a new target table until the command finishes successfully.

For metadata catalogs without plot text, add `--allow-empty-plot`. The loader
will embed title, year, and genres, and will include plot text whenever present.

## Running the API

```bash
docker build -t cinemattr-api ./api --no-cache
docker run -p 8080:8080 --env-file .env cinemattr-api
```

Or locally:

```bash
pip install -r api/requirements.txt
uvicorn --app-dir api app:app --host 0.0.0.0 --port 8080
```

## Data pipeline

The pipeline deliberately uses plain Python and DuckDB:

1. `db/imdb_datasets_import.py` imports IMDb's official non-commercial datasets.
2. `db/fetch_plots_omdb.py` resumably enriches movies with plots within your OMDb quota.
3. `db/transform.py` builds the normalized corpus, retaining existing Wikipedia plots as a fallback.
4. `api/load_embeddings.py` updates the DuckDB VSS table.

### Corpus provenance and coverage

The catalog metadata comes from IMDb's official non-commercial datasets. Plot text
is added from OMDb when an enrichment run has coverage, with existing Wikipedia
plot rows retained as a fallback. Rows without plot text are excluded by the
default embedding loader, so search coverage depends on the local enrichment
state and is not complete by default.

Run all non-enrichment stages with:

```bash
python db/run_pipeline.py --start-year 1950 --end-year 2026
```

OMDb enrichment is separate because API quotas vary:

```bash
OMDB_API_KEY=... python db/fetch_plots_omdb.py --limit 1000
python db/run_pipeline.py --skip-imdb-movies
```

## Free-hosting options (scale-to-zero friendly)

Free tiers change; verify quotas before deploy.

1. Cloud Run (container, scale-to-zero)
2. Koyeb free web service (container, may sleep)
3. Render / Railway credit-based free tiers

Oracle Always Free VM for less cold-start impact with more ops overhead.

## Local smoke check

```bash
./api/smoke_test.sh http://localhost:8080
```

## Verification

```bash
python -m compileall -q api db
python -m unittest discover -s tests -v
docker compose -f deploy/docker-compose.yml --env-file deploy/.env.example config --quiet
```

## FAQ

**Does this use LangChain or Pinecone?**
No. Both were dropped. Filter parsing is a single JSON-structured LLM call (`api/filter_parser.py`). Vector search is DuckDB VSS with an HNSW index (`api/vector_store.py`).

**Can I use a provider other than OpenAI?**
Yes. Any OpenAI-compatible endpoint works — Gemini, local models (Ollama, LM Studio), etc. Set `LLM_BASE_URL`, `LLM_API_KEY`, `LLM_MODEL` and optionally the `EMBEDDING_*` equivalents.

**Why is `/search` returning no results?**
Most likely the DuckDB VSS table is empty. You need to run `api/load_embeddings.py` against a populated movie corpus first. See "Loading the vector store" above.

**What does `EMBEDDING_DIM` do?**
It must match the output dimension of your embedding model. Mismatch causes a schema error at load time. See the cheat sheet in "Environment variables".

**Can I change the embedding model later?**
Yes, but it requires re-running `load_embeddings.py` (dimension or space changes break the existing index). Use a different `VSS_TABLE` name while rebuilding to avoid downtime.

**Is there a Lambda or serverless dependency?**
No. The backend is a plain FastAPI service (`uvicorn`/`gunicorn`) that runs anywhere containers run.
