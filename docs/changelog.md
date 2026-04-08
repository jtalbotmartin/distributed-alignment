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

---

### Task 1.2: Deterministic chunker — 2026-04-07

**What was done**:
- `src/distributed_alignment/ingest/chunker.py` — deterministic hash-based chunker that assigns sequences to chunks via `SHA-256(sequence_id) % num_chunks`, writes Parquet files with enforced schema, and produces a JSON manifest.
- Updated `src/distributed_alignment/ingest/__init__.py` to export `chunk_sequences`.
- `tests/test_chunker.py` — 19 tests across 7 test classes: hash assignment, content hashing, determinism, round-trip, distribution, manifest accuracy, Parquet schema, and edge cases.
- Added `[[tool.mypy.overrides]]` for `pyarrow.*` in `pyproject.toml` to handle missing type stubs.

**Decisions made**:
- Rows within each chunk Parquet file are sorted by `sequence_id` before writing. This is essential for determinism — without it, the same set of sequences chunked in different input orders would produce different Parquet bytes (same data, different row order). Sorting makes the output byte-identical regardless of input ordering.
- Empty chunks (no sequences hashed to that bucket) simply don't produce a Parquet file. The manifest only contains entries for non-empty chunks. This is cleaner than writing empty Parquet files and matches the TDD spec ("chunk_id → parquet_path" only for chunks that exist).
- `chunk_prefix` parameter allows distinguishing query chunks (`q000`, `q001`, ...) from reference chunks (`r000`, `r001`, ...) in the same directory structure.
- `file_checksum()` utility reads in 8KB blocks to handle large files without memory issues, and prefixes with `sha256:` for self-documenting checksums.
- The manifest is both returned as a Python object and written as JSON. The JSON file uses `model_dump(mode="json")` for clean serialisation (datetimes as ISO strings, not Python repr).

**Problems encountered**:
- The shuffle-determinism test initially failed: identical sequences ended up in the same chunks, but row ordering within the Parquet file differed because sequences were stored in insertion order. The fix was sorting each bucket by `sequence_id` before writing to Parquet.
- PyArrow lacks `py.typed` marker / type stubs, causing mypy strict to fail with `import-untyped`. Fixed by adding `[[tool.mypy.overrides]]` for `pyarrow.*` with `ignore_missing_imports = true` in `pyproject.toml`.

**Learnings**:
- Deterministic output requires more than deterministic assignment — row ordering within files also matters. If you claim "identical input → byte-identical output", you need to control every source of non-determinism, including insertion order into data structures.
- PyArrow's type story with mypy strict is still incomplete as of v23. The `ignore_missing_imports` override is the standard workaround and doesn't compromise type safety of our own code.

**Status**: Complete

---

### Task 1.3: Work package scheduler — 2026-04-07

**What was done**:
- `src/distributed_alignment/scheduler/protocols.py` — `WorkStack` Protocol class defining the interface for work package distribution: `generate_work_packages`, `claim`, `complete`, `fail`, `heartbeat`, `reap_stale`, `pending_count`, `status`.
- `src/distributed_alignment/scheduler/filesystem_backend.py` — `FileSystemWorkStack` implementation using POSIX `os.rename()` for atomic claims. Directory layout: `pending/`, `running/`, `completed/`, `poisoned/` with one JSON file per work package.
- Updated `src/distributed_alignment/scheduler/__init__.py` to export both `WorkStack` and `FileSystemWorkStack`.
- `tests/test_work_stack.py` — 26 tests across 9 test classes covering generation, claiming, completion, failure/retry, heartbeats, stale reaping, status, concurrent claims, and directory initialisation.

**Decisions made**:
- The `WorkStack` protocol uses `typing.Protocol` rather than an ABC. This is more Pythonic for structural subtyping — any class that implements the right methods satisfies the protocol without explicit inheritance. A future S3 or Redis backend just needs to implement the same methods.
- `generate_work_packages` is part of the protocol, not a standalone function. This keeps the work package lifecycle (creation through completion) on a single object, and different backends might generate packages differently (e.g. an S3 backend would write to object storage).
- `claim()` iterates `sorted(pending_dir.iterdir())` for deterministic ordering — without sorting, the iteration order is filesystem-dependent and could cause uneven claim distribution.
- `fail()` and `reap_stale()` both use the same logic pattern: increment attempt, check against max_attempts, route to PENDING or POISONED. The duplication is minimal and keeps each method self-contained.
- State transitions are logged as structured audit events with `package_id`, `from_state`, `to_state`, `worker_id`, `attempt`, `reason`, and `timestamp` — matching the TDD's audit event format.
- The write-then-unlink pattern in `complete()` and `fail()` (write to destination, then delete source) ensures the package JSON always exists in at least one directory. If the process dies between write and unlink, the package exists in both directories — recoverable, not lost.

**Problems encountered**:
- No significant problems. The atomic rename approach worked cleanly, and the concurrent claims test (10 threads, 5 packages) passed on the first run. POSIX `os.rename()` atomicity is reliable.

**Learnings**:
- `typing.Protocol` is a clean fit for the "backend interface" pattern. The protocol definition documents the contract (argument types, return types, semantics in docstrings) without imposing inheritance. mypy strict mode verifies that implementations match the protocol at usage sites.
- The write-then-unlink pattern for state transitions (write new state file, then delete old one) is a simple form of crash safety — the package is never absent from all directories. In a real production system you'd want fsync between the write and unlink, but for this project the pattern is sufficient.

**Status**: Complete

---

### Task 1.4: DIAMOND wrapper and worker — 2026-04-07

**What was done**:
- `src/distributed_alignment/worker/diamond_wrapper.py` — `DiamondWrapper` class wrapping the DIAMOND binary: `check_available()`, `make_db()`, `run_blastp()`, plus standalone `parse_output()` for parsing format 6 TSV into PyArrow Tables. `DiamondResult` dataclass for structured return values.
- `src/distributed_alignment/worker/runner.py` — `WorkerRunner` class implementing the main worker loop: claim → convert Parquet to FASTA → build reference DB → run DIAMOND blastp → parse output → write result Parquet → mark complete → repeat. Includes `parquet_chunk_to_fasta()` helper for the Parquet → FASTA conversion.
- Updated `src/distributed_alignment/worker/__init__.py` to export all public APIs.
- `scripts/generate_test_data.py` — generates synthetic protein FASTA files (no network access required). Deterministic via seed parameter.
- `tests/fixtures/diamond_output.tsv` — realistic DIAMOND format 6 output for unit testing `parse_output()`.
- `tests/test_diamond_wrapper.py` — 12 unit tests (parse output, availability checking, result dataclass, error handling) + 4 integration tests (marked `@pytest.mark.integration`).
- `tests/test_worker.py` — 9 unit tests (Parquet→FASTA conversion, worker loop with mocked DIAMOND, failure/retry, missing chunks) + 1 integration test.

**Decisions made**:
- `DiamondWrapper` is a dataclass rather than a plain class — `binary`, `threads`, `extra_args` are configuration state, not behaviour, so dataclass makes the intent clear and gives us `__init__`/`__repr__` for free.
- Exit codes use sentinel values: -1 for timeout (`subprocess.TimeoutExpired`), -2 for binary not found (`FileNotFoundError`). These are not real DIAMOND exit codes and won't collide with DIAMOND's own codes.
- `parse_output()` is a standalone function rather than a method on `DiamondWrapper`. It has no dependency on the wrapper's state (binary path, threads) and is useful independently for testing and data inspection.
- `WorkerRunner` generates its own `worker_id` via `uuid.uuid4()` hex prefix. This is simpler than requiring the caller to provide one and ensures uniqueness in multi-worker scenarios.
- The worker loop runs to exhaustion — it keeps claiming packages until `claim()` returns None. This means persistent failures exhaust all retries and eventually poison the package, which is the correct behaviour for a single-worker loop. In multi-worker scenarios, different workers would claim different packages.
- Temp working directory (`.tmp_{package_id}`) is created per package and cleaned up via `shutil.rmtree` in a `finally` block. This prevents accumulating temp files on failure.

**Problems encountered**:
- Initial test failures on the "diamond failure calls fail" tests: the worker loop kept reclaiming the same failed package until retries were exhausted (POISONED), so the assertion `status["PENDING"] == 1` after one failure was wrong. Fixed by updating tests to verify the final state (POISONED) since the worker correctly drains all retries of a persistently failing package.
- This revealed an important insight about testing the worker: you can't test "one failure" in isolation when the worker loop is autonomous — it will keep going. To test a single retry, you'd need to mock the work stack to return None after the first reclaim.

**Learnings**:
- The separation between `DiamondWrapper` (subprocess management) and `WorkerRunner` (orchestration logic) makes unit testing much cleaner. Mocking the wrapper with `MagicMock(spec=DiamondWrapper)` lets you test all the worker's claim/fail/complete logic without needing the DIAMOND binary.
- When testing loop-based workers, be careful about what "failure" means: a single DIAMOND failure doesn't mean the worker stops — it means the package gets retried. Tests need to match the actual loop semantics.
- Synthetic test data generation (deterministic via seed) is much more reliable for CI than downloading real data. The `generate_test_data.py` script produces valid protein sequences that DIAMOND can align.

**Status**: Complete

---

### Dev container and Swiss-Prot test data — 2026-04-08

**What was done**:
- `Dockerfile.dev` — minimal dev container: `python:3.11-slim` base, DIAMOND v2.1.10 via miniforge/bioconda (works on both x86_64 and arm64), uv for Python dependency management. Not the full Phase 4 production container — just enough for `pytest -m integration`.
- `docker-compose.yml` — single `dev` service with code baked into image. Usage: `docker-compose run dev uv run pytest tests/ -v`.
- `.dockerignore` — excludes `.venv`, `.git`, caches, and working directories from the Docker build context.
- `tests/fixtures/swissprot_queries.fasta` — 100 reviewed human proteins from UniProt Swiss-Prot (~84KB).
- `tests/fixtures/swissprot_reference.fasta` — 500 reviewed E. coli K-12 proteins from UniProt Swiss-Prot (~243KB).
- Updated `scripts/download_test_data.sh` to download and trim Swiss-Prot data into the fixtures directory.
- Added `integration_test_data` shared fixture in `tests/conftest.py` — uses committed Swiss-Prot fixtures when present, falls back to synthetic data if missing.
- Updated integration tests in `test_diamond_wrapper.py` and `test_worker.py` to use the shared fixture.
- Added `test_blastp_produces_hits_with_real_data` test — verifies that human vs E. coli alignment produces real homologs (e-value < 1e-5), which validates that the pipeline is producing biologically meaningful results, not just correct schema.
- Updated `README.md` with "Running tests" section covering both Docker and local approaches.

**Decisions made**:
- Pulled a minimal Dockerfile forward from Phase 4 to solve the immediate DIAMOND reproducibility problem. This is deliberately not the full production container (no Ray, no Prometheus, no Grafana) — just DIAMOND + Python + uv.
- DIAMOND installed via miniforge + bioconda rather than direct binary download. This handles cross-platform (x86_64/arm64) natively, avoiding Rosetta emulation issues on Apple Silicon. Miniforge avoids Anaconda's ToS requirements that block miniconda in non-interactive Docker builds.
- Volume mounts removed from docker-compose.yml — the iCloud Drive path (`Mobile Documents/com~apple~CloudDocs/`) causes `Resource deadlock` errors (os error 35) with Docker volume mounts. Code is baked into the image instead; rebuild with `docker-compose build dev` after changes.
- Swiss-Prot fixture data is committed to the repo (~327KB total). This removes network dependency from tests — the fixtures are always available. The download script (`scripts/download_test_data.sh`) is for refreshing them.
- Chose human (queries) vs E. coli K-12 (reference) because they're distant enough to test alignment sensitivity but share enough conserved proteins (ribosomal proteins, chaperones, metabolic enzymes) to produce meaningful hits.
- The `integration_test_data` fixture uses `shutil.copy` to a tmp_path so each test gets its own clean copy, and the committed fixtures are never modified.

**Problems encountered**:
- UniProt REST API's `size` parameter doesn't limit total results — it controls page size. Had to download all sequences and trim to the desired count with a Python script.
- DIAMOND's GitHub releases only provide `diamond-linux64` (x86_64). On Apple Silicon running Docker, this fails with `rosetta error: failed to open elf at /lib64/ld-linux-x86-64.so.2`. Switched from direct binary download to miniforge + bioconda, which provides native arm64 builds.
- Miniconda now requires accepting Anaconda Terms of Service, which fails in non-interactive Docker builds (`CondaToSNonInteractiveError`). Miniforge is the community-maintained alternative with conda-forge as default channel and no ToS requirement.
- Docker volume mounts from iCloud Drive paths cause `Resource deadlock would occur (os error 35)`. Removed the volume mount from docker-compose.yml; code is baked into the image instead.

**Learnings**:
- Real test data catches things synthetic data doesn't. The `test_blastp_produces_hits_with_real_data` test verifies biological correctness (e-value < 1e-5 between human and E. coli), not just schema correctness. Synthetic random sequences may not produce any hits at all.
- Committing small fixture files (~300KB) to the repo is the right trade-off for test reproducibility. It eliminates network dependencies and makes CI deterministic.
- The dev Dockerfile pattern (minimal, pinned deps, single purpose) is a good intermediate step before the full production container. It solves the immediate problem without scope-creeping into Phase 4 infrastructure.
- For Docker on macOS with iCloud Drive projects: don't use volume mounts. Bake code into the image and rebuild on changes. The trade-off (rebuild vs live reload) is worth it for avoiding filesystem compatibility issues.
- Miniforge is the right choice for bioinformatics Docker images over miniconda — same functionality, no ToS friction, conda-forge as default channel.

**Status**: Complete

---

### Task 1.5: Result merger — 2026-04-08

**What was done**:
- `src/distributed_alignment/merge/merger.py` — `merge_query_chunk()` function that reads per-ref-chunk result Parquet files, renames DIAMOND columns to MergedHit model names, deduplicates (best evalue per query-subject pair), applies global top-N ranking per query, validates the output schema, and writes merged Parquet. Uses DuckDB for all SQL operations.
- `MERGED_SCHEMA` exported as a PyArrow schema constant matching the MergedHit model.
- Updated `src/distributed_alignment/merge/__init__.py` to export `merge_query_chunk` and `MERGED_SCHEMA`.
- `tests/test_merger.py` — 14 tests across 7 test classes: global ranking, tiebreaking, deduplication, top-N filtering, schema validation, incomplete merge detection, empty results, and return value. All tests use synthetic Parquet fixtures — no DIAMOND required.

**Decisions made**:
- **Column name normalisation**: The worker writes DIAMOND's native column names (`qseqid`, `sseqid`, `pident`, `length`, `mismatch`, `gapopen`, ...) while the MergedHit model uses descriptive names (`query_id`, `subject_id`, `percent_identity`, `alignment_length`, `mismatches`, `gap_opens`, ...). The merger renames via DuckDB `AS` clauses in the SQL. This keeps the worker simple (raw DIAMOND output) and puts the normalisation at the boundary where it belongs.
- **Two-step dedup then rank**: First `ROW_NUMBER() OVER (PARTITION BY query_id, subject_id ORDER BY evalue ASC, bitscore DESC)` to deduplicate, then a second `ROW_NUMBER() OVER (PARTITION BY query_id ORDER BY evalue ASC, bitscore DESC)` for global ranking. This ensures dedup happens before top-N filtering — otherwise a duplicate could consume a top-N slot.
- **Completeness check before merge**: The `expected_ref_chunks` parameter lists which ref chunks should have results. If any are missing, the function raises with a clear error listing the missing chunks. This prevents silently producing partial results.
- **Empty results are valid**: If all result files exist but contain zero rows, the output is a valid empty Parquet file with the correct schema. This is correct behaviour — it means no alignments were found, which is a legitimate result.
- Added `query_chunk_id` and `ref_chunk_id` columns to the merged output as string literals injected in the DuckDB SQL. These enable tracing any hit back to its source work package.

**Problems encountered**:
- DuckDB's `fetch_arrow_table()` is deprecated in v1.5; replacement is `.arrow()` which returns a `RecordBatchReader`, not a `Table`. Needed `.arrow().read_all()` to get a PyArrow Table that can be passed to `pq.write_table()`. Initial attempt with just `.arrow()` caused `TypeError: expected pyarrow.lib.Table, got pyarrow.lib.RecordBatchReader`.
- The `CAST(global_rank AS INTEGER)` in the SQL was necessary because DuckDB's `ROW_NUMBER()` returns `BIGINT` by default, but the MERGED_SCHEMA expects `int32`. The explicit cast in SQL plus `arrow_table.cast(MERGED_SCHEMA)` at the PyArrow level ensures type consistency.

**Learnings**:
- DuckDB's ability to `read_parquet()` directly in SQL and return Arrow tables makes it an excellent fit for this kind of merge operation — no intermediate pandas DataFrames, no serialisation overhead.
- Column name normalisation at stage boundaries (DIAMOND names → model names) is a form of data contract enforcement. It's better to do this explicitly at the boundary than to let DIAMOND's naming conventions leak through the rest of the pipeline.
- Testing mergers with synthetic Parquet fixtures (known evalues, known query-subject pairs) is much more effective than testing with real DIAMOND output where you can't easily predict exact values.

**Status**: Complete
