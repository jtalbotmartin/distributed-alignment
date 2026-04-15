# `distributed-alignment`

![CI](https://github.com/jtalbotmartin/distributed-alignment/actions/workflows/ci.yml/badge.svg)

A distributed, fault-tolerant protein sequence alignment system built around DIAMOND BLAST.

Decomposes large-scale alignment problems into independent work packages, distributes them across elastic workers via Ray, and produces ML-ready feature tables — with full observability and infrastructure as code.

## Status

Phase 2 (Fault Tolerance & Distribution) complete. See [`docs/task-tracker.md`](docs/task-tracker.md) for progress.

## Documentation

- [User Requirements](docs/01-user-requirements.md)
- [Technical Design](docs/02-technical-design.md)
- [Product Requirements](docs/03-product-requirements.md)
- [Architecture Decision Records](docs/adr/)

## Running with Docker

The easiest way to run the full pipeline:

```bash
# 1. Prepare input data
mkdir -p data/input
cp tests/fixtures/swissprot_queries.fasta data/input/queries.fasta
cp tests/fixtures/swissprot_reference.fasta data/input/reference.fasta

# 2. Build images
docker-compose build

# 3. Ingest: parse and chunk FASTA files
docker-compose run --rm ingest

# 4. Start workers + monitoring
docker-compose up -d worker prometheus grafana

# 5. Watch progress
docker-compose logs -f worker

# 6. Scale workers
docker-compose up -d --scale worker=4

# 7. Check status
docker-compose run --rm worker status --work-dir /data/work

# 8. View dashboard
open http://localhost:3000

# 9. Stop everything
docker-compose down
```

- **Grafana dashboard**: `http://localhost:3000` (anonymous access)
- **Prometheus**: `http://localhost:9091`
- Workers auto-restart on failure (`restart: on-failure`)
- Killed workers' packages are reclaimed by the reaper in surviving workers

## Local Development

### Prerequisites

- Python 3.11+, [uv](https://docs.astral.sh/uv/), GNU Make (optional)
- [DIAMOND](https://github.com/bbuchfink/diamond/wiki) for integration tests (or use Docker)

### Setup

```bash
uv sync
```

### Run the pipeline locally

```bash
make ingest ARGS="--queries data/queries.fasta --reference data/reference.fasta --output-dir work/"
make run ARGS="--work-dir work/"
make status ARGS="--work-dir work/"

# Multi-worker
make run ARGS="--work-dir work/ --workers 4"

# Ray backend (requires: uv add 'distributed-alignment[ray]')
make run ARGS="--work-dir work/ --workers 4 --backend ray"
```

### Running tests

```bash
make test               # Unit tests (no DIAMOND needed)
make test-integration   # Integration tests (requires DIAMOND)
make test-all           # All local tests
make test-docker        # Full suite in Docker (DIAMOND + Ray)
make lint               # Ruff + mypy --strict
```

Run `make help` to see all available commands.
