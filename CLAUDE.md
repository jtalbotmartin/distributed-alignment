# Claude Code Instructions for `distributed-alignment`

## Project Context

This is a distributed protein sequence alignment orchestrator. Read the full technical design
at `docs/02-technical-design.md` and the current task list at `docs/task-tracker.md`.

## Working Conventions

### Code Style
- Python 3.11+, strict mypy, ruff for linting/formatting
- Type hints on all function signatures (no `Any` unless unavoidable)
- Pydantic v2 for all data models
- structlog for all logging (no print statements, no stdlib logging)
- Google-style docstrings on public functions and classes

### Testing
- pytest for all tests
- hypothesis for property-based tests on data transformations and state machines
- Mark tests that require DIAMOND with `@pytest.mark.integration`
- Fixtures in `tests/conftest.py` for shared test data
- Aim for >80% coverage on core modules

### Dependencies
- Managed via `uv` and `pyproject.toml`
- Add new dependencies with `uv add <package>`
- Dev dependencies with `uv add --dev <package>`

### File Organisation
- Source code in `src/distributed_alignment/`
- All shared Pydantic models in `src/distributed_alignment/models.py`
- Config in `src/distributed_alignment/config.py`
- Each component is a subpackage (ingest/, scheduler/, worker/, etc.)

### Changelog
- After completing a task, write an entry to `docs/changelog.md` following the format defined there
- Document: what was done, decisions made during implementation, problems encountered, learnings

### What NOT to do
- Don't create files outside the project directory
- Don't install packages globally — use `uv add`
- Don't use print() for logging — use structlog
- Don't use Python's built-in hash() for chunking — it's randomised per process. Use hashlib.
- Don't write CSV/TSV output — use Parquet via PyArrow
- Don't skip type hints to save time
