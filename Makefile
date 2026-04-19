# Developer workflow targets.
# Run `make help` to see available commands.

PYTHONPATH := src
export PYTHONPATH

.PHONY: help setup test test-integration test-all test-docker docker-build docker-build-prod lint cli compute-embeddings run-tier1 update-tier1-fixtures clean-tier1 regenerate-accession-map

help: ## Show this help
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
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

# Tier 1 end-to-end run via Docker (DIAMOND + full pipeline on committed
# fixtures).  Outputs go to /tmp/tier1_run/.  Committed fixture snapshots
# live in tests/fixtures/tier1_outputs/; regenerate them after a code change
# with: make run-tier1 && make update-tier1-fixtures
#
# --force-rerun is fine here because the Makefile target is for
# reproducibility automation — the user-facing safety guard on
# --force-rerun is against accidental CLI overwrites, not scripted runs.
TIER1_WORK_DIR ?= /tmp/tier1_run
TIER1_RUN_ID ?= tier1_baseline

run-tier1: docker-build-prod ## End-to-end Tier 1 pipeline run (Docker)
	@mkdir -p $(TIER1_WORK_DIR)
	@echo "Pre-building taxonomy DuckDB..."
	@PYTHONPATH=src uv run python -c "from pathlib import Path; from distributed_alignment.taxonomy import TaxonomyDB; TaxonomyDB.from_ncbi_dump(Path('tests/fixtures/taxonomy/nodes.dmp'), Path('tests/fixtures/taxonomy/names.dmp'), Path('tests/fixtures/taxonomy/accession2taxid.tsv'), Path('$(TIER1_WORK_DIR)/taxonomy.duckdb')).close()"
	@echo "Ingesting Tier 1 fixtures..."
	docker run --rm \
		-v "$(shell pwd)/tests/fixtures:/fixtures:ro" \
		-v $(TIER1_WORK_DIR):/work \
		distributed-alignment:latest \
		ingest \
			--queries /fixtures/metagenome_queries.fasta \
			--reference /fixtures/diverse_reference.fasta \
			--output-dir /work
	@echo "Running pipeline..."
	docker run --rm \
		-v "$(shell pwd)/tests/fixtures:/fixtures:ro" \
		-v $(TIER1_WORK_DIR):/work \
		distributed-alignment:latest \
		run \
			--work-dir /work \
			--taxonomy-db /work/taxonomy.duckdb \
			--embeddings /fixtures/query_embeddings.parquet \
			--run-id $(TIER1_RUN_ID) \
			--force-rerun
	@echo "Done. Outputs in $(TIER1_WORK_DIR)/features/$(TIER1_RUN_ID)/"
	@echo "To update committed fixtures: make update-tier1-fixtures"

docker-build-prod: ## Build the production Docker image (DIAMOND + pipeline)
	docker build -t distributed-alignment:latest .

update-tier1-fixtures: ## Copy Tier 1 run outputs into tests/fixtures/tier1_outputs/
	@mkdir -p tests/fixtures/tier1_outputs
	cp $(TIER1_WORK_DIR)/features/$(TIER1_RUN_ID)/combined_features.parquet tests/fixtures/tier1_outputs/
	cp $(TIER1_WORK_DIR)/features/$(TIER1_RUN_ID)/alignment_features.parquet tests/fixtures/tier1_outputs/
	cp $(TIER1_WORK_DIR)/features/$(TIER1_RUN_ID)/kmer_features.parquet tests/fixtures/tier1_outputs/
	cp $(TIER1_WORK_DIR)/features/$(TIER1_RUN_ID)/enriched.parquet tests/fixtures/tier1_outputs/
	cp $(TIER1_WORK_DIR)/catalogue.duckdb tests/fixtures/tier1_outputs/
	@du -sh tests/fixtures/tier1_outputs/

clean-tier1: ## Remove the Tier 1 working directory
	rm -rf $(TIER1_WORK_DIR)

regenerate-accession-map: ## Rebuild tests/fixtures/taxonomy/accession2taxid.tsv from FASTA headers
	uv run python scripts/regenerate_accession_map.py
