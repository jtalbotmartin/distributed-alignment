# `distributed-alignment` — Implementation Task Tracker

> Working document. Updated as tasks are completed.

---

## Phase 1: Core Pipeline (MVP)

Goal: working end-to-end pipeline on a single machine with a small dataset.
Ingest → chunk → schedule → align (single worker) → merge → Parquet output.

---

### Task 1.0: Project scaffolding

**What**: Set up the repo structure, pyproject.toml, basic config, empty module layout.

**Files to create**:
- `pyproject.toml` — project metadata, dependencies, dev dependencies, tool config (ruff, mypy, pytest)
- `distributed_alignment.toml` — default config file
- `src/distributed_alignment/__init__.py`
- `src/distributed_alignment/config.py` — Pydantic Settings config class
- `src/distributed_alignment/models.py` — shared Pydantic models (ProteinSequence, WorkPackage, ChunkManifest, etc.)
- `src/distributed_alignment/cli.py` — Typer CLI skeleton with subcommands (ingest, run, status, explore)
- `tests/conftest.py` — shared fixtures (tmp dirs, small test FASTA data)
- `.gitignore`
- `README.md` — minimal, will be expanded in Phase 5

**Dependencies** (initial):
- Runtime: pydantic, pydantic-settings, pyarrow, duckdb, typer, structlog, rich
- Dev: pytest, pytest-cov, hypothesis, mypy, ruff

**Acceptance criteria**:
- `uv sync` creates working virtualenv
- `uv run python -c "import distributed_alignment"` works
- `uv run distributed-alignment --help` shows CLI with subcommands
- `uv run pytest` passes (even if tests are trivial/empty)
- `uv run ruff check src/` clean
- `uv run mypy src/ --strict` clean

---
