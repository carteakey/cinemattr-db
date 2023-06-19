# cinemattr-db

Backend and database of [cinemattr.ca](https://github.com/carteakey/cinemattr.ca)

## How it works

- Self querying retriever using LangChain, on a pinecone database containing popular movies (>1000 imdb rating count) released since 1950 to date.
- OpenAI LLM translates user input to a vector database query.
- Initial filtering is done through metadata columns (title, year, rating, actors etc.) using operators like > < = AND,OR.
- Semantic search is done on the plot and summaries extracted for each movie.
- Final 20 results (titles) are sent back as a response.
- API hosted on an rate-limited AWS Lambda function.

## How data is collected and loaded

- Movie details and plot summaries are scraped from IMDb and Wikipedia. (`db/airflow/dags/scrapers`)
- Airflow is used to orchestrate scraping jobs for every year. (`db/airflow`)
- Data is loaded to a duckdb instance. (`db/duckdb`)
- DBT is used for data transformation and cleanup (Clean text, create final tables, merge data from both sources) (`db/dbt`)
- Plot summaries are loaded into a pinecone vector database (see `api/load.ipynb`).
- Vector Embeddings are either 
   - HuggingFace `all-mpnet-base-v2`, Free - `api/hf_embeddings`
   - OpenAI `text-embedding-ada-002`, Paid - $0.0001/1000 tokens - `api`

## Building and Testing the Lambda API

Build

```bash
docker build -t cinemattr-api . --no-cache  --platform=linux/arm64
```

```bash
docker build -t cinemattr-api .  --platform=linux/arm64
```

Run

```bash
docker run -p 9000:8080  --env-file .env.dev cinemattr-api
```

Test query

```bash

curl -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" -d '{
        "queryStringParameters":
            { "query" : "owen wilson wow"}
    }'
```

Auth ECR

```bash
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin $LAMBDA_ECR_REPO
```

Create Docker Repository

```bash
aws ecr create-repository --repository-name cinemattr-api --image-scanning-configuration scanOnPush=true --image-tag-mutability MUTABLE  --region us-east-1
```

Tag latest build

```bash
docker tag cinemattr-api $LAMBDA_ECR_REPO/cinemattr-api:latest
```

Push

```bash
docker push $LAMBDA_ECR_REPO/cinemattr-api:latest
```

Test Lambda Function URL

```
curl "$LAMBDA_API_URL?query=query"
```

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


