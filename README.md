# cinemattr-db

Backend and database of cinemattr.ca

## How it works
- Self querying retriever using LangChain, on a pinecone database containing popular movies released since 1950 to date.
- OpenAI LLM translates user input to a vector database query.
- Initial filtering is done through metadata columns (title, year, rating, actors etc.) using operators like > < = AND,OR.
- Semantic search is done on the plot and summaries extracted for each movie.
- Final 20 results (titles) are sent back as a response.
- API hosted on an rate-limited AWS Lambda function.

## How data is collected and loaded
- Movie details and plot summaries are scraped from IMDb and Wikipedia.
- Airflow is used to orchestrate scraping jobs for every year.
- Data is loaded to a duckdb instance.
- DBT is used for data transformation and cleanup (Clean text, create final tables, merge data from both sources)
- Plot summaries are loaded into a pinecone vector database (see `cinemattr.ipynb`)

## Building and Testing the Lambda API

Build
```bash
docker build -t cinemattr-api . --no-cache  --platform=linux/amd64
```
Run
```bash
docker run -p 9000:8080  --env-file .env.dev cinemattr-api
```
Test query
```bash
curl -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" -d '{"query": "owen wilson wow"}'
```
