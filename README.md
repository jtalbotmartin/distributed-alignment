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
- GNU Make (optional, for shortcut commands)

### Setup

```bash
uv sync
```

### Run the pipeline

```bash
# Using make (recommended)
make ingest ARGS="--queries data/queries.fasta --reference data/reference.fasta --output-dir work/"
make run ARGS="--work-dir work/"
make status ARGS="--work-dir work/"

# Or directly
PYTHONPATH=src uv run python -m distributed_alignment ingest \
    --queries data/queries.fasta \
    --reference data/reference.fasta \
    --output-dir work/
PYTHONPATH=src uv run python -m distributed_alignment run --work-dir work/
PYTHONPATH=src uv run python -m distributed_alignment status --work-dir work/
```

### Running tests

```bash
make test               # Unit tests (no DIAMOND needed)
make test-integration   # Integration tests (requires DIAMOND)
make test-all           # Everything
make lint               # Ruff + mypy --strict
```

**Or via Docker (no local DIAMOND needed):**

```bash
docker-compose build dev
docker-compose run --rm dev uv run pytest tests/ -v
```

Run `make help` to see all available commands.
