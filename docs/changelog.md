# `distributed-alignment` — Changelog & Learnings Log

> Living document tracking implementation decisions, problems encountered, and learnings during development.
> Written to during each task so the reasoning behind the code is preserved.

---

## Format

Each entry follows this structure:

```
### Task X.Y: <name> — <date>

**What was done**: Brief summary of files created/modified.

**Decisions made**: Any design choices that came up during implementation and why.

**Problems encountered**: Errors, unexpected behaviour, things that didn't work first time.

**Learnings**: Anything worth remembering — patterns that worked well, gotchas, things to do differently next time.

**Status**: Complete / In progress / Blocked
```

---

## Log

### Task 1.0: Project scaffolding — 2026-04-07

**What was done**: Created the full project scaffolding:
- `pyproject.toml` with runtime deps (pydantic, pydantic-settings[toml], pyarrow, duckdb, typer, structlog, rich) and dev deps (pytest, pytest-cov, hypothesis, mypy, ruff). Uses hatchling build backend with `src/` layout.
- `distributed_alignment.toml` default config file with all pipeline settings.
- `src/distributed_alignment/__init__.py` with version.
- `src/distributed_alignment/config.py` — `DistributedAlignmentConfig` using Pydantic Settings with `DA_` env prefix and TOML file source.
- `src/distributed_alignment/models.py` — All shared Pydantic models: `ProteinSequence`, `ChunkManifest`, `ChunkEntry`, `WorkPackage`, `WorkPackageState`, `MergedHit`, `FeatureRow`.
- `src/distributed_alignment/cli.py` — Typer CLI with `ingest`, `run`, `status`, `explore` subcommands (stubs).
- `__init__.py` for all subpackages (ingest, scheduler, worker, merge, taxonomy, features, catalogue, observability, explorer).
- `tests/conftest.py` with shared fixtures (`work_dir`, `sample_fasta`, `sample_sequences`).
- `tests/test_scaffolding.py` with 14 smoke tests covering config, models, validators, and fixtures.

**Decisions made**:
- Used `pydantic-settings[toml]` extra and overrode `settings_customise_sources` to properly configure TOML loading. The `toml_file` key in `SettingsConfigDict` alone doesn't register the TOML source — you need to explicitly add `TomlConfigSettingsSource` to the sources tuple.
- TOML config uses flat keys (no `[distributed_alignment]` section) since the settings model is flat. A nested TOML section would require either a nested model or env_nested_delimiter matching.
- Used `StrEnum` instead of `str, Enum` for `WorkPackageState` per Python 3.11+ convention (ruff UP042).
- `datetime` import kept outside `TYPE_CHECKING` block with `# noqa: TCH003` — Pydantic needs it at runtime for field validation despite `from __future__ import annotations`.
- Typer `[all]` extra no longer exists in v0.24+, dropped to plain `typer>=0.12`.

**Problems encountered**:
- Pydantic's `from __future__ import annotations` + `TYPE_CHECKING` block pattern doesn't work for types used in model fields — Pydantic needs them at runtime to build validators. The error was `WorkPackage is not fully defined; you should define datetime`.
- The TOML config source needed explicit wiring via `settings_customise_sources`. Without it, `toml_file` in `model_config` emits a warning and is silently ignored.
- The `[distributed_alignment]` TOML section header caused `Extra inputs are not permitted` because pydantic-settings saw a nested dict key that didn't match any field.

**Learnings**:
- Pydantic Settings v2 separates config *declaration* (`model_config`) from source *registration* (`settings_customise_sources`). The TOML file path goes in config, but you must add the source class to the sources tuple for it to actually load.
- ruff's `TCH003` rule (move stdlib imports to TYPE_CHECKING) conflicts with Pydantic models that need runtime access to those types. Suppress with `# noqa: TCH003` on the specific import.

**Status**: Complete

---

### Task 1.1: Streaming FASTA parser — 2026-04-07

**What was done**:
- `src/distributed_alignment/ingest/fasta_parser.py` — streaming generator-based FASTA parser that yields validated `ProteinSequence` objects one at a time.
- Updated `src/distributed_alignment/ingest/__init__.py` to export `parse_fasta`.
- `tests/test_fasta_parser.py` — 20 tests organised into 5 test classes: valid parsing, empty files, error handling, max-length warnings, generator behaviour.
- `tests/fixtures/` — 4 fixture FASTA files: `valid.fasta`, `empty.fasta`, `malformed.fasta`, `invalid_chars.fasta`.
- Fixed `sample_sequences` fixture in `conftest.py` (3rd sequence was 53 chars, not 52).

**Decisions made**:
- Parser delegates all amino acid validation and case normalisation to the existing `ProteinSequence` model — no duplicated validation logic.
- `_build_sequence` helper wraps Pydantic `ValueError` with line number and sequence ID context, so errors are actionable ("Line 5: sequence 'P12345': Invalid amino acid characters: ['1', '2', '3']").
- Blank lines between sequences are silently skipped (common in real-world FASTA files).
- `max_length` parameter defaults to 100,000; set to 0 to disable. Exceeding it logs a structlog warning but still yields the sequence — this is informational, not a hard failure.
- `Generator` and `Path` imports moved into `TYPE_CHECKING` block since they're only used in annotations (safe with `from __future__ import annotations`). This is different from `datetime` in `models.py` which Pydantic needs at runtime.

**Problems encountered**:
- The conftest `sample_sequences` fixture had length 52 for the third sequence, but the actual string is 53 characters. Caught by the parser tests — a good example of why tests against real parsing (not hand-counted fixtures) matter.
- The `.pth` file / editable install issue resurfaced: editing source files invalidated the cached install. `rm -rf .venv && uv sync` remains the reliable fix when working from an iCloud path with spaces.

**Learnings**:
- The `TCH003` rule (move to TYPE_CHECKING) is safe for types only used in annotations when `from __future__ import annotations` is active, but not for types Pydantic needs at runtime. The key distinction: annotations-only imports → TYPE_CHECKING block; Pydantic field types → keep at runtime.
- FASTA parsing is deceptively simple until you handle edge cases: multi-line sequences, blank lines, empty sequences, data before headers, empty headers. Covering these in tests upfront is much easier than debugging them later.

**Status**: Complete
