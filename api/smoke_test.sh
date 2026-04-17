#!/usr/bin/env bash
set -euo pipefail

API_BASE_URL="${1:-http://localhost:8080}"

echo "Checking health endpoint..."
curl --fail --silent --show-error "${API_BASE_URL}/health" > /dev/null

echo "Checking search endpoint..."
curl --fail --silent --show-error \
	-X POST "${API_BASE_URL}/search" \
	-H "Content-Type: application/json" \
	-d '{"query":"mind bending thriller with dream layers"}' > /dev/null

echo "Smoke checks passed."
