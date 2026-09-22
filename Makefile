.DEFAULT_GOAL := help

.PHONY: help install sync run test isolation-break isolation-fixed build db-up db-down benchmark clean

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

isolation-break: ## Demonstrate stream-limit corruption (expected to fail).
	uv run python tests/test_isolation.py unsafe

isolation-fixed: ## Verify locking prevents stream-limit corruption.
	uv run python tests/test_isolation.py fixed

build: ## Build source and wheel distributions.
	uv build

db-up: ## Start PostgreSQL using docker compose.
	docker compose up -d db

db-down: ## Stop the PostgreSQL container.
	docker compose down

benchmark: ## Benchmark bulk session closing (requires DATABASE_URL).
	uv run python benchmarks/streaming_session_close.py

clean: ## Remove generated Python build artifacts.
	@find . -type d \( -name __pycache__ -o -name .pytest_cache \) -prune -exec rm -rf {} +
	@find . -type d -name '*.egg-info' -prune -exec rm -rf {} +
	@rm -rf build dist
