# Pravaha - convenience targets. See README for details.
.DEFAULT_GOAL := help
SHELL := /bin/bash

help: ## show targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

up: ## build + start the full stack
	docker compose up -d --build

down: ## stop (keep volumes)
	docker compose down

clean: ## stop + wipe volumes (clean-start)
	docker compose down -v

migrate: ## run alembic migrations
	docker compose run --rm migrate

seed: ## generate demo traffic (10 min @ 40 ev/s)
	docker compose --profile demo up seed

logs: ## tail key services
	docker compose logs -f api analytics anomaly ai-worker

test-unit: ## fast tests, no Docker
	pytest tests/unit tests/failure -q

test-integration: ## testcontainers integration tests
	pytest -m integration -q

lint: ## ruff + frontend lint/typecheck
	ruff check pravaha tests
	cd apps/web && npm run lint && npm run typecheck

fe-build: ## build the dashboard
	cd apps/web && npm run build

scenario-%: ## run an incident scenario, e.g. `make scenario-payment_failure_spike`
	python -m pravaha.scripts.scenarios run $* --bootstrap

loadtest: ## measured load sweep -> load-test-results/
	python -m pravaha.scripts.load_test --sweep 100,500,1000 --duration 30 --bootstrap --out load-test-results/run.json

.PHONY: help up down clean migrate seed logs test-unit test-integration lint fe-build loadtest
