#!/usr/bin/env bash
# run.sh — build and run the full stack via Rancher Desktop (docker compose)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Rancher Desktop puts docker/nerdctl here — add it in case it's not on PATH yet
export PATH="$HOME/.rd/bin:$PATH"

# Point docker to Rancher Desktop's socket (not Docker Desktop's)
export DOCKER_HOST="unix://$HOME/.rd/docker.sock"

# ── 1. Check New Relic license key ──────────────────────────────────────────
if grep -q 'license_key:.*YOUR_NEW_RELIC_LICENSE_KEY' "$SCRIPT_DIR/newrelic.yml"; then
    echo ""
    echo "ERROR: New Relic license key not set."
    echo "  1. Open newrelic.yml"
    echo "  2. Replace 'YOUR_NEW_RELIC_LICENSE_KEY' with your actual key"
    echo "  Get your key at: https://one.newrelic.com/api-keys"
    echo ""
    exit 1
fi

# ── 2. Build and start everything ───────────────────────────────────────────
echo "Building and starting Kafka + app via Rancher Desktop..."
echo "  (First run pulls images and compiles — takes ~2-3 min)"
echo ""

docker compose -f "$SCRIPT_DIR/docker-compose.yml" up --build
