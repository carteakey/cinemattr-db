# cinemattr-db

Backend and database of [cinemattr.ca](https://github.com/carteakey/cinemattr.ca)

## How it works

- Self querying retriever using LangChain, on a pinecone database containing popular movies (>1000 imdb rating count) released since 1950 to date.
- An OpenAI-compatible LLM translates user input to a vector database query.
- Initial filtering is done through metadata columns (title, year, rating, actors etc.) using operators like > < = AND,OR.
- Semantic search is done on the plot and summaries extracted for each movie.
- Final 20 results (titles) are sent back as a response.
- API hosted as a standalone Python HTTP service (FastAPI).

## Search API (no Lambda dependency)

### API routes

- `GET /health`
- `POST /search` with JSON body: `{ "query": "..." }`
- `GET /search?query=...` (compat path)

Example:

```bash
curl -X POST "http://localhost:8080/search" \
  -H "Content-Type: application/json" \
  -d '{"query":"mind bending sci-fi with alternate realities"}'
```

### Provider and model environment variables

```bash
# LLM
LLM_BASE_URL=https://api.openai.com/v1
LLM_API_KEY=...
LLM_MODEL=gpt-4.1-mini

# Embeddings
EMBEDDING_PROVIDER=openai-compatible
EMBEDDING_BASE_URL=
EMBEDDING_API_KEY=
EMBEDDING_MODEL=text-embedding-3-small
HF_EMBEDDING_DEVICE=

# Pinecone
PINECONE_API_KEY=...
PINECONE_ENV=...
VECTOR_INDEX_NAME=cinemattr
VECTOR_TOP_K=20

# API hardening
RATE_LIMIT_REQUESTS=30
RATE_LIMIT_WINDOW_SECONDS=60
ALLOWED_ORIGINS=*

# Only trust X-Forwarded-For when your API sits behind a trusted proxy/load balancer.
TRUST_PROXY_HEADERS=false
TRUSTED_PROXY_IPS=
```

`EMBEDDING_PROVIDER=huggingface` enables local `sentence-transformers` embeddings. Hosted providers still require an API key; keyless mode is only intended for local endpoints such as `localhost`.

### Pinecone index migration notes (embedding model upgrades)

When changing embedding model dimensions, do not overwrite the active index in place.

1. Create a new index name (for example `cinemattr-v2`) with the target dimension.
2. Re-embed and load data into that new index.
3. Switch `VECTOR_INDEX_NAME` to the new index.
4. Verify search quality/latency via smoke checks.
5. Keep the previous index for rollback until confidence is high, then delete it.

## How data is collected and loaded

- Movie details and plot summaries are scraped from IMDb and Wikipedia. (`db/airflow/dags/scrapers`)
- Airflow is used to orchestrate scraping jobs for every year. (`db/airflow`)
- Data is loaded to a duckdb instance. (`db/duckdb`)
- DBT is used for data transformation and cleanup (Clean text, create final tables, merge data from both sources) (`db/dbt`)
- Plot summaries are loaded into a pinecone vector database (see `api/load.ipynb`).
- Vector embeddings can run in either mode:
  - Hugging Face `sentence-transformers/all-mpnet-base-v2` via `EMBEDDING_PROVIDER=huggingface` and the `api/hugging_face` image
  - OpenAI-compatible embeddings via `EMBEDDING_PROVIDER=openai-compatible`

## Building and running the API container

Build

```bash
docker build -t cinemattr-api ./api --no-cache
```

Hugging Face embedding image

```bash
docker build -t cinemattr-api-hf ./api/hugging_face --no-cache
```

Run

```bash
docker run -p 8080:8080 --env-file .env.example cinemattr-api
```

Test query

```bash
curl -X POST "http://localhost:8080/search" \
  -H "Content-Type: application/json" \
  -d '{"query":"owen wilson wow"}'
```

Auth ECR

```bash
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin $LAMBDA_ECR_REPO
```

Create Docker Repository

```bash
aws ecr create-repository --repository-name cinemattr-api --image-scanning-configuration scanOnPush=true --image-tag-mutability MUTABLE  --region us-east-1
```

Tag/push steps stay the same if using ECR-based deploys.

## Airflow

```bash
docker exec -it airflow-airflow-webserver-1 sh
```

## Setting up data pipeline

Initialize environment
```
cd db
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Init Duckdb database
```
cd duckdb
python -m utils init
```

Init Airflow 
```
cd airflow
docker compose up -d
```

Export final movies plot table
```bash
python -m utils db.duckdb export_movies
```

## Free-hosting options (scale-to-zero friendly)

Free tiers change often; verify quotas before deploy:

1. Cloud Run (container, scale-to-zero)
2. Koyeb free web service (container, may sleep)
3. Render/Railway credit-based free tiers (if available)

For less cold-start impact with higher ops overhead, use Oracle Always Free VM.

## Local smoke checks

After running the API:

```bash
./api/smoke_test.sh
```
