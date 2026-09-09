#!/usr/bin/env bash
# Runs once when the Codespace / dev container is created.
set -euo pipefail

echo "==> Pravaha dev container bootstrap"

# Local .env for docker compose (safe dev defaults; override AI_* to use a real provider)
if [ ! -f .env ]; then
  cp .env.example .env
  echo "    wrote .env from .env.example"
fi

# Python venv for running tests / scripts on the host side of the container
python3.11 -m venv .venv
./.venv/bin/pip install --upgrade pip -q
./.venv/bin/pip install -e ".[dev]" -q
echo "    python venv ready ($(./.venv/bin/python --version))"

# Frontend deps (so 'npm run dev' / lint / build work without the image)
if [ -d apps/web ]; then
  (cd apps/web && npm install --no-audit --no-fund --silent) || true
fi

cat <<'EOF'

==> Ready.

Start the whole platform:

    docker compose up -d --build
    docker compose run --rm migrate
    docker compose --profile demo up -d seed     # optional: demo traffic

Then open the forwarded port 3000 (login admin@pravaha.local / admin12345).
API docs on port 8000 -> /docs.

Trigger the marquee demo:

    docker compose run --rm --no-deps seed \
      python -m pravaha.scripts.scenarios run payment_failure_spike --api http://api:8000 --bootstrap

EOF
