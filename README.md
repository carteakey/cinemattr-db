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
# LLM (filter parser)
LLM_BASE_URL=https://api.openai.com/v1
LLM_API_KEY=...
LLM_MODEL=gpt-4.1-mini

# Embeddings (falls back to LLM_* if unset)
EMBEDDING_BASE_URL=
EMBEDDING_API_KEY=
EMBEDDING_MODEL=text-embedding-3-small
EMBEDDING_DIM=1536          # must match the model's output dim

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

Set `EMBEDDING_DIM` to match. Changing dim requires reloading the VSS table.

## Loading the vector store

Assumes you already have a DuckDB database with scraped movies/plots (see "Data pipeline" below).

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

- Movie details + plots scraped from IMDb and Wikipedia (`db/airflow/dags/scrapers`).
- Airflow orchestrates per-year scraping jobs (`db/airflow`).
- Raw data loaded into DuckDB (`db/duckdb`).
- DBT transforms (`db/dbt`).
- Embeddings loaded into the VSS table via `api/load_embeddings.py`.

### Airflow year range

```bash
CINEMATTR_START_YEAR=1950
CINEMATTR_END_YEAR=2026
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
