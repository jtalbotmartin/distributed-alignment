# Developer workflow targets.
# Run `make help` to see available commands.

PYTHONPATH := src
export PYTHONPATH

.PHONY: help setup test test-integration test-all test-docker docker-build lint cli compute-embeddings

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

setup: ## Install dependencies
	uv sync

test: ## Run unit tests (no DIAMOND needed)
	uv run pytest tests/ -v -m "not integration"

test-integration: ## Run integration tests (requires DIAMOND)
	uv run pytest tests/ -v -m integration

test-all: ## Run all local tests (unit + integration)
	uv run pytest tests/ -v

docker-build: ## Build the dev Docker image
	docker-compose build dev

test-docker: docker-build ## Run full suite in Docker (DIAMOND + Ray)
	docker-compose run --rm dev

lint: ## Run linting and type checking
	uv run ruff check src/ tests/
	uv run mypy src/ --strict

cli: ## Show CLI help
	uv run python -m distributed_alignment --help

# Pipeline commands — pass ARGS for additional arguments
# e.g.: make ingest ARGS="--queries data/q.fasta --reference data/r.fasta"
ingest: ## Run ingest command
	uv run python -m distributed_alignment ingest $(ARGS)

run: ## Run alignment pipeline
	uv run python -m distributed_alignment run $(ARGS)

status: ## Show pipeline status
	uv run python -m distributed_alignment status $(ARGS)

EMBED_FASTA ?= tests/fixtures/metagenome_queries.fasta
EMBED_OUTPUT ?= tests/fixtures/query_embeddings.parquet
EMBED_RUN_ID ?= fixture-tier1

compute-embeddings: ## Compute ESM-2 embeddings (override with EMBED_FASTA=... EMBED_OUTPUT=...)
	uv run --extra embeddings python scripts/compute_embeddings.py \
		--fasta $(EMBED_FASTA) \
		--output $(EMBED_OUTPUT) \
		--run-id $(EMBED_RUN_ID)
