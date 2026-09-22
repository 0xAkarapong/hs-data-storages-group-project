.DEFAULT_GOAL := help

.PHONY: help install sync run test isolation-break isolation-fixed build db-up db-down redis-up benchmark benchmark-redis benchmark-ping clean

help: ## Show available commands.
	@awk 'BEGIN {FS = ":.*##"} /^[a-zA-Z_-]+:.*##/ {printf "%-18s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

install: ## Create/update the uv environment with development dependencies.
	uv sync --all-groups

sync: install ## Alias for install.

run: ## Seed the database with the demo flow (requires DATABASE_URL).
	uv run python seed.py

test: ## Run the PostgreSQL-backed test suite (requires a running database).
	uv run python tests/test_cancel_event.py
	uv run python tests/test_concurrency.py
	uv run python tests/test_isolation.py fixed
	uv run python tests/test_record_ping.py

isolation-break: ## Demonstrate stream-limit corruption (expected to fail).
	uv run python tests/test_isolation.py unsafe

isolation-fixed: ## Verify locking prevents stream-limit corruption.
	uv run python tests/test_isolation.py fixed

build: ## Build source and wheel distributions.
	uv build

db-up: ## Start PostgreSQL using docker compose.
	docker compose up -d db

db-down: ## Stop the Postgres and Redis containers.
	docker compose down

redis-up: ## Start Redis using docker compose.
	docker compose up -d redis

benchmark: ## Benchmark bulk session closing (requires DATABASE_URL).
	uv run python benchmarks/streaming_session_close.py

benchmark-redis: ## Benchmark Redis's raw INCR ceiling (requires REDIS_URL / redis-up).
	uv run python benchmarks/redis_ping_baseline.py

benchmark-ping: ## Benchmark record_ping on one hot event, local Postgres only (requires db-up).
	uv run python benchmarks/oltp_ping_baseline.py

clean: ## Remove generated Python build artifacts.
	@find . -type d \( -name __pycache__ -o -name .pytest_cache \) -prune -exec rm -rf {} +
	@find . -type d -name '*.egg-info' -prune -exec rm -rf {} +
	@rm -rf build dist
