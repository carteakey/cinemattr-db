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
