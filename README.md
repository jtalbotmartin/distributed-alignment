# `distributed-alignment`

A distributed, fault-tolerant protein sequence alignment system built around DIAMOND BLAST.

Decomposes large-scale alignment problems into independent work packages, distributes them across elastic workers via Ray, and produces ML-ready feature tables — with full observability and infrastructure as code.

## Status

Phase 1 (Core Pipeline MVP) complete. See [`docs/task-tracker.md`](docs/task-tracker.md) for progress.

## Documentation

- [User Requirements](docs/01-user-requirements.md)
- [Technical Design](docs/02-technical-design.md)
- [Product Requirements](docs/03-product-requirements.md)
- [Architecture Decision Records](docs/adr/)

## Quick Start

### Prerequisites

- Python 3.11+
- [uv](https://docs.astral.sh/uv/) for dependency management
- [DIAMOND](https://github.com/bbuchfink/diamond/wiki) for alignment (or use Docker)

### Setup

```bash
uv sync
```

### Run the pipeline

```bash
# 1. Ingest: parse and chunk FASTA files
distributed-alignment ingest \
    --queries data/queries.fasta \
    --reference data/reference.fasta \
    --output-dir work/

# 2. Run: align, then merge results
distributed-alignment run --work-dir work/

# 3. Status: check pipeline progress
distributed-alignment status --work-dir work/
```

### Running tests

**Docker (recommended — no local DIAMOND needed):**

```bash
docker-compose build dev
docker-compose run --rm dev uv run pytest tests/ -v
```

**Local:**

```bash
# Unit tests (no DIAMOND needed)
uv run pytest tests/ -m "not integration" -v

# Integration tests (requires DIAMOND)
uv run pytest tests/ -m integration -v

# All tests
uv run pytest tests/ -v
```

**Linting and type checking:**

```bash
uv run ruff check src/ tests/
uv run mypy src/ --strict
```
