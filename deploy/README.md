# Deploy — Oracle Always Free VM + Cloudflare Tunnel

No open ports. No cert management. Cloudflare Tunnel handles HTTPS.

---

## Prerequisites

- Oracle Always Free VM provisioned (Ubuntu 22.04, 1 OCPU, 1 GB RAM)
- Domain on Cloudflare (free plan)
- Fresh OMDB API key from [omdbapi.com](https://www.omdbapi.com/apikey.aspx) (free tier: 1,000 req/day)
- Gemini API key from [aistudio.google.com](https://aistudio.google.com) (free tier sufficient)

---

## Part 1 — Build the database locally

### 1. Install Python deps

```bash
cd /Users/kchauhan/repos/cinemattr-db
python -m venv .venv-pipeline
source .venv-pipeline/bin/activate
pip install -r db/requirements-pipeline.txt
```

### 2. Import IMDb datasets (134k movies, 1950–2026)

```bash
python db/run_pipeline.py --start-year 1950 --end-year 2026 \
  --skip-transform --skip-vss
# Downloads ~500 MB from datasets.imdbws.com to db/duckdb/imdb_cache/
# Runtime: ~5 min
```

### 3. Fetch plot text from OMDB (1,000/day free tier)

```bash
OMDB_API_KEY=<your_key> python db/fetch_plots_omdb.py \
  --limit 5000 --min-votes 10000
# Resumable — re-run daily until you have enough plots
# ~5,000 plots takes ~5 days on free tier
```

### 4. Run transform (builds movie_plots_ld view)

```bash
python db/run_pipeline.py --start-year 1950 --end-year 2026 \
  --skip-imdb-movies --skip-vss
```

### 5. Load embeddings into DuckDB VSS

```bash
EMBEDDING_BASE_URL=https://generativelanguage.googleapis.com/v1beta/openai/ \
EMBEDDING_API_KEY=<gemini_key> \
EMBEDDING_MODEL=gemini-embedding-001 \
EMBEDDING_DIM=768 \
python api/load_embeddings.py \
  --source db/duckdb/movies.duckdb \
  --table movie_plots_ld \
  --id-col source --plot-col plot \
  --title-col title --year-col year \
  --genre-col genre --rating-col imdb_rating
# Output: data/cinemattr.duckdb  (~200 MB with 5k embeddings)
```

### 6. Smoke-test locally

```bash
DUCKDB_PATH=data/cinemattr.duckdb \
LLM_API_KEY=<gemini_key> \
LLM_BASE_URL=https://generativelanguage.googleapis.com/v1beta/openai/ \
LLM_MODEL=gemini-2.0-flash-lite \
EMBEDDING_API_KEY=<gemini_key> \
EMBEDDING_BASE_URL=https://generativelanguage.googleapis.com/v1beta/openai/ \
EMBEDDING_MODEL=gemini-embedding-001 \
EMBEDDING_DIM=768 \
.venv-pipeline/bin/uvicorn api.app:app --host 0.0.0.0 --port 8082

curl http://localhost:8082/health   # → {"status":"ok","indexed_rows":N}
curl -s -X POST http://localhost:8082/search \
  -H 'Content-Type: application/json' \
  -d '{"query":"a mind-bending heist inside dreams"}' | python -m json.tool
```

---

## Part 2 — Oracle VM setup (one-time)

### 1. SSH into the VM

```bash
ssh ubuntu@<oracle-vm-ip>
```

### 2. Install Docker

```bash
curl -fsSL https://get.docker.com | sh
sudo usermod -aG docker $USER
newgrp docker          # reload group without re-login
```

### 3. Clone repo

```bash
git clone https://github.com/carteakey/cinemattr-db
cd cinemattr-db
```

---

## Part 3 — Cloudflare Tunnel (one-time)

1. Go to [Cloudflare Zero Trust dashboard](https://one.dash.cloudflare.com) → **Networks → Tunnels**
2. **Create tunnel** → name: `cinemattr-api` → **Docker** connector
3. Copy the tunnel token (starts with `eyJ...`)
4. Add **Public Hostname**:
   - Subdomain: `api`
   - Domain: `cinemattr.ca` (or your domain)
   - Service: `http://api:8080`
5. Save

---

## Part 4 — Deploy

### 1. Copy the database from local

```bash
# From your local machine:
scp data/cinemattr.duckdb ubuntu@<oracle-vm-ip>:~/cinemattr-db/data/
```

### 2. Configure environment

```bash
# On Oracle VM:
cd ~/cinemattr-db
cp deploy/.env.example deploy/.env
nano deploy/.env
```

Fill in:
```
LLM_API_KEY=<gemini_key>
EMBEDDING_API_KEY=<gemini_key>
CLOUDFLARE_TUNNEL_TOKEN=<token_from_step_3>
```

### 3. Start services

```bash
cd ~/cinemattr-db/deploy
docker compose up -d --build
docker compose ps          # both services should be "running"
```

### 4. Verify

```bash
curl https://api.cinemattr.ca/health
# → {"status":"ok","indexed_rows":N}
```

---

## Part 5 — Wire the frontend

In [Vercel project settings](https://vercel.com/dashboard) → **Settings → Environment Variables**:

```
SEARCH_API_URL = https://api.cinemattr.ca
```

Trigger a redeployment:
```bash
# Or just push any commit to main — Vercel auto-deploys
```

---

## Operations

```bash
# Logs
docker compose logs -f api

# Restart API
docker compose restart api

# Update after git pull
docker compose up -d --build api

# Health check
curl https://api.cinemattr.ca/health
```

---

## Rebuild corpus on VM (alternative to scp)

If the VM has enough RAM to run the pipeline directly:

```bash
cd ~/cinemattr-db
pip install -r db/requirements-pipeline.txt
python db/run_pipeline.py --start-year 1950 --end-year 2026 --skip-vss
OMDB_API_KEY=xxx python db/fetch_plots_omdb.py --limit 5000
EMBEDDING_API_KEY=xxx ... python api/load_embeddings.py \
  --source db/duckdb/movies.duckdb \
  --table movie_plots_ld \
  --id-col source --plot-col plot \
  --title-col title --year-col year \
  --genre-col genre --rating-col imdb_rating
docker compose restart api
```
