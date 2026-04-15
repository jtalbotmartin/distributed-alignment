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
- Docker (for Ray tests and full reproducibility)
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

Multi-worker and Ray backend:
```bash
# Local multiprocessing (4 workers)
make run ARGS="--work-dir work/ --workers 4"

# Ray backend (requires ray: uv add 'distributed-alignment[ray]')
make run ARGS="--work-dir work/ --workers 4 --backend ray"
```

### Running tests

```bash
make test               # Unit tests only (no DIAMOND, no Ray)
make test-integration   # Integration tests (requires DIAMOND locally)
make test-all           # All local tests (unit + integration)
make test-docker        # Full suite in Docker (DIAMOND + Ray, recommended)
make lint               # Ruff + mypy --strict
```

`make test-docker` builds the dev Docker image (includes DIAMOND and Ray) and runs the complete test suite (~205 tests). This is the most reliable way to run all tests, including Ray integration tests which require Docker due to `uv run` / Ray environment conflicts.

Run `make help` to see all available commands.

### Monitoring

Start the monitoring stack alongside the pipeline:

```bash
# Start Prometheus + Grafana
docker-compose up -d prometheus grafana

# Run the pipeline (metrics exposed on port 9090)
make run ARGS="--work-dir work/"

# View the dashboard
open http://localhost:3000
```

- **Grafana**: `http://localhost:3000` (anonymous access, no login needed)
- **Prometheus**: `http://localhost:9091`
- Dashboard: "distributed-alignment Pipeline" — auto-provisioned with panels for pipeline progress, package duration, throughput, errors, and estimated cost.
- When using the Ray backend, metrics are automatically aggregated across actors via Ray's built-in metrics system.
