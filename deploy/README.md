# Deploy — Oracle Always Free VM + Cloudflare Tunnel

No open ports. No cert management. Cloudflare Tunnel handles HTTPS.

## One-time setup

### 1. Oracle VM
Create an Always Free VM (Ubuntu 22.04). Install Docker:
```bash
curl -fsSL https://get.docker.com | sh
sudo usermod -aG docker $USER
```

### 2. Cloudflare Tunnel
In Cloudflare Zero Trust dashboard (free):
1. Networks → Tunnels → Create tunnel → name: `cinemattr-api`
2. Copy the tunnel token
3. Add a Public Hostname: `api.yourdomain.com` → `http://api:8080`

### 3. Deploy
```bash
git clone https://github.com/carteakey/cinemattr-db
cd cinemattr-db

# Copy VSS database (built locally via db/run_pipeline.py)
scp data/cinemattr.duckdb oracle-vm:~/cinemattr-db/data/

# Set env vars
cp deploy/.env.example deploy/.env
# edit deploy/.env — fill LLM_API_KEY, EMBEDDING_API_KEY, CLOUDFLARE_TUNNEL_TOKEN

cd deploy
docker compose up -d --build
```

### 4. Wire frontend
Set `SEARCH_API_URL=https://api.yourdomain.com` in Vercel project env vars.

## Operations

```bash
# Logs
docker compose logs -f api

# Restart
docker compose restart api

# Update (after git pull)
docker compose up -d --build api

# Health check
curl https://api.yourdomain.com/health
```

## Rebuild VSS database on VM

If you want to run the pipeline directly on the VM instead of copying the DB:
```bash
pip install -r db/requirements-pipeline.txt
python db/imdb_datasets_import.py --start-year 1950 --end-year 2026
# get OMDB key, then:
OMDB_API_KEY=xxx python db/fetch_plots_omdb.py --limit 5000
python db/transform.py
source deploy/.env && python api/load_embeddings.py \
  --source db/duckdb/movies.duckdb \
  --table movie_plots_ld \
  --id-col source --plot-col plot --title-col title \
  --year-col year --genre-col genre --rating-col imdb_rating
```
