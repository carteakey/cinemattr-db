# Cinemattr revival plan (no Lambda dependency)

## Problem statement
Revive the project with a modern, maintainable stack that removes AWS Lambda coupling, improves model quality, keeps Pinecone for now, supports OpenAI-compatible endpoints via environment variables (including local providers), prioritizes free hosting paths, and adds practical quality-of-life upgrades.

## Current state (from codebase)
- Frontend (`cinemattr.ca`) is SvelteKit on Vercel adapter and proxies search through `src/routes/api/getResults/+server.ts` using `LAMBDA_API_URL`.
- Search backend (`cinemattr-db/api/app.py`) is Lambda handler style (`lambda_handler`) with LangChain `SelfQueryRetriever`, Pinecone, and `gpt-3.5-turbo`.
- Backend still contains Lambda-specific behavior (`LAMBDA_TASK_ROOT`, `/tmp` model copy patterns, Lambda Docker CMD).
- Random prompt endpoint (`src/routes/api/getRandomPrompt/+server.ts`) is also pinned to `gpt-3.5-turbo`.
- Error handling is brittle in key places (e.g., `getResults` route catches and logs but may return no response shape).
- Data pipeline already supports Pinecone embedding loads (OpenAI and HF DAG variants), so Pinecone can remain as-is during first migration.

## Target architecture
Hybrid deployment:
1. **Frontend**: keep SvelteKit app.
2. **Backend**: migrate to a standalone Python HTTP API (FastAPI) deployed as a container service (not Lambda).
3. **Model access**: use OpenAI-compatible client config via env vars so provider can be switched without code changes.
4. **Vector store**: keep Pinecone initially; use index versioning for safe model/embedding upgrades.

### Core backend env contract (provider-agnostic)
- `LLM_BASE_URL` (OpenAI-compatible endpoint; optional for native OpenAI)
- `LLM_API_KEY`
- `LLM_MODEL` (query parsing / metadata translation model)
- `EMBEDDING_BASE_URL` (optional, if different endpoint)
- `EMBEDDING_API_KEY`
- `EMBEDDING_MODEL`
- `VECTOR_INDEX_NAME`
- `VECTOR_TOP_K`

## Model direction (quality upgrade)
- Replace hardcoded `gpt-3.5-turbo` with env-driven default:
  - Parsing/default: `gpt-4.1-mini` (or equivalent OpenAI-compatible model).
  - Embeddings default: `text-embedding-3-small` (balanced quality/cost), with optional high-quality profile (`text-embedding-3-large`) and local profile via OpenAI-compatible local endpoints.
- Introduce index versioning (`cinemattr-v1`, `cinemattr-v2`) to avoid dimension mismatch outages during embedding model switches.

## Free-hosting-first deployment options (no always-on guarantee acceptable)
Primary options to evaluate and document with current quotas at implementation time:
1. **Google Cloud Run (scale-to-zero)** for Python API container.
2. **Koyeb or similar free web service** for API with sleep/cold-start behavior.
3. **Railway/Render-like credit-based tiers** if available in region.

Fallback:
- **Oracle Always Free VM** for more uptime reliability (more ops overhead).

Decision rule:
- Pick the option that supports container deploys + env vars + HTTPS with lowest operational burden, then record exact limits in docs.

## Implementation plan (phased todos)

### Phase 1: Replace Lambda runtime with HTTP API
- **todo: backend-fastapi-runtime**
  - Convert `cinemattr-db/api/app.py` from Lambda handler to FastAPI endpoints (`/health`, `/search`).
  - Keep retrieval logic equivalent first, then harden behavior.
- **todo: backend-provider-abstraction**
  - Add env-driven OpenAI-compatible client config and remove hardcoded model/provider assumptions.
  - Ensure local endpoints can be used by env override only.
- **todo: backend-error-contract**
  - Return structured error payloads and status codes; remove ambiguous empty responses.

### Phase 2: Frontend integration and QoL reliability
- **todo: frontend-api-rewire**
  - Replace `LAMBDA_API_URL` contract with new API base URL (e.g., `SEARCH_API_URL`).
  - Update `getResults` route to preserve explicit error shapes for UI.
- **todo: frontend-random-prompt-modernize**
  - Move random prompt route to same provider-agnostic model config and newer default model.
- **todo: frontend-qol-error-feedback**
  - Improve user-visible messaging for upstream errors/retries and show actionable states.

### Phase 3: Abuse protection and operational safety
- **todo: backend-rate-limiting**
  - Add IP-based rate limiting/token bucket middleware at backend edge.
  - Make limits configurable via env for free-tier tuning.
- **todo: request-hardening**
  - Add timeouts, retry policy boundaries, and input validation limits.

### Phase 4: Local setup and deployment paths
- **todo: local-dev-experience**
  - Add a simple local run path (frontend + backend + env examples) with minimal commands.
- **todo: deploy-free-tier-targets**
  - Add deployment manifests/docs for at least two free-tier options.
  - Capture known cold-start and sleep behavior.
- **todo: docs-and-faq-refresh**
  - Update READMEs and FAQ text to remove Lambda references and reflect new architecture.

### Phase 5: Verification and migration safety
- **todo: smoke-checks**
  - Add lightweight smoke checks for `/health`, `/search`, and frontend integration path.
- **todo: pinecone-index-migration-plan**
  - Document/index version rollout + rollback procedure when changing embedding models.

## Notes and considerations
- Keep Pinecone in first pass to constrain migration risk.
- Avoid major retrieval algorithm redesign until runtime/provider migration is stable.
- If local models underperform for metadata parsing, keep parser model remote while retaining local embedding option.
- Explicitly avoid silent fallbacks: if provider/model config is invalid, fail with clear error payloads.

## Progress update

Completed:
- Migrated backend from Lambda handler style to FastAPI HTTP service (`/health`, `/search`), with structured error payloads.
- Added provider-agnostic OpenAI-compatible env config for LLM + embeddings, and replaced hardcoded `gpt-3.5-turbo` defaults.
- Rewired frontend search route to `SEARCH_API_URL` with retry/timeout controls and deterministic error handling.
- Updated random prompt endpoint to provider-agnostic config and modern default model.
- Added backend IP-based rate limiting and request hardening controls via env vars.
- Added backend smoke check script (`api/smoke_test.sh`).
- README section describing free-hosting options (Cloud Run / Koyeb / Render / Oracle).

## Phase 6 — drop LangChain + migrate to DuckDB VSS (post-Pinecone)

Rationale: the old Pinecone index/credentials are gone, and the LangChain stack (SelfQueryRetriever + vector wrapper + SQLiteCache) was overkill for a single retrieval endpoint. Replacing both with a small, targeted stack keeps the code honest and local-first.

New stack:
- **LLM filter parser** — one JSON-structured OpenAI-compatible call (`api/filter_parser.py`) replaces `SelfQueryRetriever` + `lark`. Returns `{semantic_query, year_from, year_to, genres, min_rating}`.
- **Embeddings** — direct `openai` client (`api/embeddings.py`), works with OpenAI / Gemini / local providers.
- **Vector store** — DuckDB VSS (HNSW, cosine) in a single local `.duckdb` file (`api/vector_store.py`).
- **Loader** — `api/load_embeddings.py` reads from a source DuckDB table and upserts embeddings into the VSS table. Idempotent.
- Dependencies dropped: `langchain`, `lark`, `pinecone-client`, `cohere`, `SQLiteCache`.

Still open:
- Deploy manifests (Cloud Run / Koyeb yaml) are still README-only.
- End-to-end smoke against a populated VSS table is pending (requires running the loader against the existing DuckDB corpus).
- Decide: keep `api/hugging_face/` image once we re-introduce a local embedding profile, or leave dropped.
