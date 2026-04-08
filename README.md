# `distributed-alignment`

A distributed, fault-tolerant protein sequence alignment system built around DIAMOND BLAST.

Decomposes large-scale alignment problems into independent work packages, distributes them across elastic workers via Ray, and produces ML-ready feature tables — with full observability and infrastructure as code.

## Status

🚧 Under active development. See [`docs/task-tracker.md`](docs/task-tracker.md) for progress.

## Documentation

- [User Requirements](docs/01-user-requirements.md)
- [Technical Design](docs/02-technical-design.md)
- [Product Requirements](docs/03-product-requirements.md)
- [Architecture Decision Records](docs/adr/)
### Running tests (Docker — recommended)

No local dependencies needed beyond Docker:

```bash
# Build the dev container (includes DIAMOND + Python deps)
docker-compose build dev

# Run all tests (unit + integration, DIAMOND included)
docker-compose run dev uv run pytest tests/ -v

# Run only integration tests
docker-compose run dev uv run pytest tests/ -m integration -v
```

Code is baked into the image at build time. After editing source files,
rebuild with `docker-compose build dev` to pick up changes.

### Running tests (local)

```bash
# Install Python dependencies
uv sync

# Run unit tests (no DIAMOND needed)
uv run pytest tests/ -v

# For integration tests, install DIAMOND first:
#   macOS:  brew install diamond
#   Linux:  see https://github.com/bbuchfink/diamond/wiki
# Then:
uv run pytest tests/ -m integration -v
```

### Linting and type checking

```bash
uv run ruff check src/ tests/
uv run mypy src/ --strict
```
