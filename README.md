# cinemattr-db

docker build -t cinemattr-api . --no-cache  --platform=linux/amd64
docker run -p 9000:8080  --env-file .env.dev cinemattr-api
curl -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" -d '{"query": "test"}'
