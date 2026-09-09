#!/usr/bin/env bash
# One-shot deploy of Pravaha onto a fresh Linux host (Ubuntu/Debian; x86_64 or
# arm64 - e.g. an Oracle Cloud Always-Free Ampere A1 VM).
#
#   git clone https://github.com/HxrshRaj/Pravaha && cd Pravaha
#   cp deploy/.env.prod.example .env && nano .env        # fill in the required values
#   sudo bash deploy/deploy.sh
#
# It installs Docker (if missing), pulls the prebuilt GHCR images, runs
# migrations + topic creation, and brings the stack up behind Caddy (auto-HTTPS).
set -euo pipefail
cd "$(dirname "$0")/.."

if [ ! -f .env ]; then
  echo "!! .env not found. Run: cp deploy/.env.prod.example .env  then edit it." >&2
  exit 1
fi
set -a; . ./.env; set +a
: "${PRAVAHA_DOMAIN:?set PRAVAHA_DOMAIN in .env}"
: "${API_JWT_SECRET:?set API_JWT_SECRET in .env (openssl rand -hex 32)}"
: "${POSTGRES_PASSWORD:?set POSTGRES_PASSWORD in .env}"
: "${BOOTSTRAP_ADMIN_PASSWORD:?set BOOTSTRAP_ADMIN_PASSWORD in .env}"

if ! command -v docker >/dev/null 2>&1; then
  echo "==> installing Docker"
  curl -fsSL https://get.docker.com | sh
  systemctl enable --now docker
fi

COMPOSE=(docker compose -f docker-compose.yml -f docker-compose.prod.yml)

echo "==> pulling images"
"${COMPOSE[@]}" pull --quiet postgres redis kafka caddy || true
"${COMPOSE[@]}" pull --quiet api web || {
  echo "!! could not pull prebuilt images. Either the 'images' workflow has not"
  echo "   run yet, or the packages are private. Fix: make the GHCR packages"
  echo "   public, or set IMAGE_REPO/IMAGE_TAG in .env, or build locally:"
  echo "     ${COMPOSE[*]} build"
  exit 1
}

echo "==> infrastructure"
"${COMPOSE[@]}" up -d postgres redis kafka
echo "==> waiting for kafka + postgres to be healthy"
for i in $(seq 1 40); do
  ok=$("${COMPOSE[@]}" ps --format '{{.Service}}:{{.Health}}' | tr '\n' ' ')
  case "$ok" in
    *kafka:healthy*postgres:healthy*|*postgres:healthy*kafka:healthy*) break ;;
  esac
  sleep 5
done

echo "==> migrations + topics"
"${COMPOSE[@]}" run --rm migrate
"${COMPOSE[@]}" run --rm kafka-init

echo "==> full stack"
"${COMPOSE[@]}" up -d

echo
echo "==> done. https://${PRAVAHA_DOMAIN}  (login ${BOOTSTRAP_ADMIN_EMAIL:-admin@pravaha.local})"
echo "    logs:   ${COMPOSE[*]} logs -f api analytics anomaly ai-worker"
echo "    demo:   ${COMPOSE[*]} run --rm --no-deps seed \\"
echo "              python -m pravaha.scripts.scenarios run payment_failure_spike --api http://api:8000 --bootstrap"
