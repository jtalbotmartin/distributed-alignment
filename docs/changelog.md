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

---

### Task 1.6: Structured logging setup — 2026-04-08

**What was done**:
- `src/distributed_alignment/observability/logging.py` — `configure_logging()` function that sets up structlog with JSON or console output, binds `run_id` globally via contextvars, integrates with stdlib logging, and is idempotent (safe to call multiple times).
- Updated `src/distributed_alignment/observability/__init__.py` to export `configure_logging`.
- Updated all six existing modules to bind a `component` to their loggers:
  - `fasta_parser.py` → `component="ingest"`
  - `chunker.py` → `component="chunker"`
  - `filesystem_backend.py` → `component="scheduler"`
  - `diamond_wrapper.py` → `component="diamond"`
  - `runner.py` → `component="worker"`
  - `merger.py` → `component="merger"`
- `tests/test_logging.py` — 13 tests across 5 classes: JSON output format, run_id correlation, bound context, log level filtering, and idempotency.

**Decisions made**:
- Used `structlog.contextvars` for `run_id` binding rather than module-level `bind()`. Contextvars propagate across all loggers in the same thread/async context without requiring each module to explicitly bind the run_id. Calling `configure_logging(run_id=...)` once at startup makes the run_id appear in every log entry from every module.
- JSON output auto-detected via `sys.stdout.isatty()` — JSON for production/CI (stdout not a TTY), coloured console for interactive development. Can be overridden explicitly with the `json_output` parameter.
- Integrated structlog with stdlib logging via `ProcessorFormatter`. This means third-party library logs (DuckDB, PyArrow) also flow through structlog's formatting pipeline and include the same timestamp format and JSON structure.
- `cache_logger_on_first_use=False` ensures that reconfiguring logging (e.g. changing run_id) takes effect immediately on existing logger instances.
- Component binding uses `structlog.get_logger(component="name")` at module level. This is bound at logger creation time, so every log call from that module automatically includes the component field.

**Problems encountered**:
- structlog's `add_log_level` processor adds a key named `"level"`, not `"log_level"`. Initial tests expected `"log_level"` based on the structlog docs mentioning "log_level" in some contexts, but the actual JSON output uses `"level"`.

**Learnings**:
- structlog's `contextvars` integration is the cleanest way to add cross-cutting context (like `run_id`) that should appear in every log entry without passing it through function arguments. It works like thread-local storage but is async-safe.
- Testing logging configuration by redirecting handler streams to `StringIO` is more reliable than capturing stderr — it gives direct access to the formatted output for JSON parsing.

**Status**: Complete

---

### Task 1.7: End-to-end integration and CLI wiring — 2026-04-08

**What was done**:
- Fully implemented `src/distributed_alignment/cli.py` with three working subcommands:
  - `ingest` — parses FASTA files, chunks both query and reference, writes manifests to the work directory.
  - `run` — reads manifests, generates work packages, runs a single DIAMOND worker, merges results per query chunk.
  - `status` — reads manifests and work stack state, displays a rich-formatted summary table.
- `tests/test_integration.py` — end-to-end integration test exercising the full pipeline: ingest → chunk → schedule → align → merge → DuckDB query. Uses Swiss-Prot test data with 2 query chunks × 2 ref chunks (4 work packages). Verifies all packages complete, merged Parquet has correct schema, results are queryable, and biologically meaningful hits exist (evalue < 1e-5).
- Updated `README.md` with complete quickstart: prerequisites, setup, running the pipeline (3 CLI commands), running tests (Docker and local), linting.

**Decisions made**:
- The work directory has a consistent structure: `chunks/{queries,references}/`, `work_stack/{pending,running,completed,poisoned}/`, `results/`, `merged/`, plus `query_manifest.json` and `ref_manifest.json` at the root. The `ingest` command creates the chunks and manifests, the `run` command creates everything else.
- Chunk count is computed from `total_sequences // chunk_size`, not passed directly. This means the `--chunk-size` flag controls target chunk size rather than number of chunks — more intuitive for users who think in terms of "how big should each chunk be?" rather than "how many chunks?".
- The `run` command accepts `--workers N` but only supports 1 in Phase 1 (prints a warning if >1 is requested). This keeps the CLI interface forward-compatible with Phase 2's multi-worker support.
- `configure_logging(json_output=False)` in CLI commands so users see human-readable output. The integration test uses `json_output=True` for structured assertion.
- The `status` command uses `rich.Table` for a formatted work package state display.
- The integration test calls Python functions directly rather than CLI subprocesses — faster, easier to debug, and tests the same code paths.

**Problems encountered**:
- The persistent iCloud `.pth` file issue continues to surface whenever `uv sync` creates a new .venv: the editable install's `.pth` file points to the correct `src/` directory but Python doesn't process it due to the path containing spaces. `rm -rf .venv && uv sync` remains the fix. This is a known limitation of developing on iCloud Drive — the Docker path avoids it entirely.

**Phase 1 rough edges to address in Phase 2**:
- Single worker only — the `run` command processes all packages sequentially. Phase 2 adds multi-worker support via Ray.
- No heartbeat/reaper during the run — the worker runs synchronously, so stale heartbeat detection isn't exercised. Phase 2's concurrent workers will need the reaper running in a background thread.
- The `ingest` command counts sequences twice (once to determine chunk count, once to actually chunk). This could be optimised with a streaming approach that starts chunking immediately, but for Phase 1 with small datasets the double-pass is fine.
- Manifest paths are absolute — works locally but won't be portable across machines. Could be made relative to work_dir.
- No `--resume` support for the `run` command — if interrupted, you currently need to clear the work_stack and re-run. Phase 2 should detect existing work packages and resume.

**Status**: Complete — Phase 1 MVP is done.

---

### Post-Phase 1 cleanup — 2026-04-08

**What was done**:
- **Ref DB caching**: `WorkerRunner` now caches built `.dmnd` files in `ref_dbs/` directory. Multiple query chunks aligning against the same reference chunk reuse the cached database instead of rebuilding. With 10 query × 2 ref chunks, this cuts `makedb` calls from 20 to 2. Cache hit/miss logged via structlog.
- **Removed dead code**: `_read_package()` method in `FileSystemWorkStack` was defined but never called — all callers read JSON directly. Removed.
- **Fixed `explore` exit code**: Changed from `raise typer.Exit(code=1)` to printing a "not yet implemented" message and exiting cleanly (code 0).
- **Added CLI unit tests**: 14 new tests in `tests/test_cli.py` using `typer.testing.CliRunner` — covers help output for all subcommands, ingest with valid/invalid args, status with/without data, run validation (missing manifests, workers warning), and explore stub.
- **Fixed pytest `pythonpath`**: Added `pythonpath = ["src"]` to `[tool.pytest.ini_options]` in `pyproject.toml`. This ensures `pytest` can import the package regardless of whether the editable install's `.pth` file is working — permanently fixes the iCloud path issue for tests.

**Decisions made**:
- Ref DB cache uses the simple pattern: check if `ref_dbs/{ref_chunk_id}.dmnd` exists, skip `makedb` if so. This is safe because chunk content is deterministic (same chunk ID = same sequences, verified by content checksums in the manifest). No cache invalidation needed within a single pipeline run.
- CLI tests use `typer.testing.CliRunner` which invokes the app in-process — no subprocess overhead, no DIAMOND dependency for most tests. Only the `run` command needs DIAMOND, and validation tests (missing manifests, workers warning) stop before reaching DIAMOND.

**Status**: Complete

---

### Permanent fix for iCloud .pth import issue — 2026-04-08

**What was done**:
- Set `package = false` in `[tool.uv]` in `pyproject.toml`. This tells uv to not install the project as a Python package at all — no editable install, no `.pth` file, no import failures on iCloud paths.
- Created `src/distributed_alignment/__main__.py` as CLI entry point, enabling `python -m distributed_alignment` as the invocation method.
- Created `Makefile` with targets for all common operations: `setup`, `test`, `test-integration`, `test-all`, `lint`, `cli`, `ingest`, `run`, `status`. The Makefile sets `PYTHONPATH=src` so imports work without any package installation.
- Updated `README.md` with `make` commands as the primary workflow.

**Root cause analysis**:
Python's `site.py` processes `.pth` files from `site-packages/` at startup. These files contain directory paths to add to `sys.path`. However, paths containing spaces (like iCloud's `Mobile Documents/com~apple~CloudDocs/`) are silently dropped — `site.py` doesn't error, it just doesn't add the path. This meant the editable install's `.pth` file (which pointed to our `src/` directory) was useless. The symptom: `ModuleNotFoundError: No module named 'distributed_alignment'` on every CLI invocation, intermittently depending on whether `uv run` had recently re-synced.

Multiple workarounds were tried and failed:
- `rm -rf .venv && uv sync` — worked temporarily, broke again after iCloud synced
- `uv sync --no-editable` — installed correctly, but `uv run` re-synced to editable mode
- `uv.toml` with `no-editable = true` — not a valid config key
- `.env` with `UV_NO_EDITABLE=1` — uv doesn't read `.env` for its own config

**Solution**:
`package = false` stops uv from creating the broken `.pth` file entirely. Instead:
- **pytest** uses `pythonpath = ["src"]` (from `pyproject.toml`) — this was already in place
- **CLI** uses `PYTHONPATH=src uv run python -m distributed_alignment` — the Makefile handles this transparently
- **Docker** is unaffected (no iCloud path)

**Trade-off**: After code changes, `uv sync` no longer reinstalls the project (there's nothing to install). For pytest, this is transparent — `pythonpath` always reads from `src/`. For the CLI, the Makefile's `PYTHONPATH=src` always reads from `src/`. This is actually simpler than the editable install approach.

**Status**: Complete

---

## Phase 2: Fault Tolerance & Distribution

### Task 2.0: Wire config into CLI — 2026-04-09

**What was done**:
- Added Phase 2 fields to `DistributedAlignmentConfig`: `backend` (`Literal["local", "ray"]`, default `"local"`) and `reaper_interval` (default 60 seconds). Existing fields `heartbeat_interval`, `heartbeat_timeout`, `max_attempts` were already present.
- Created `load_config()` function in `config.py` that handles TOML file discovery (searches `work_dir` first, then cwd), applies env var overrides, and merges explicit CLI overrides. Overrides with value `None` are ignored, so CLI flags only take effect when explicitly provided.
- Rewired all CLI subcommands (`ingest`, `run`, `status`) to use `load_config()` instead of hard-coded defaults. CLI flags now default to `None` so they only override config when the user explicitly passes them.
- Updated `distributed_alignment.toml` with all settings, grouped by category, with inline comments explaining each field.
- `tests/test_config.py` — 16 new tests across 6 test classes: defaults, Phase 2 fields, TOML loading, env var overrides, `load_config` with TOML discovery and override precedence, CLI config integration.

**Decisions made**:
- CLI flag defaults are `None`, not the config defaults. This is the standard pattern for "user explicitly provided vs using default" — if the CLI value is `None`, the config file / env var / default applies. If set, the CLI flag wins. Pydantic Settings' init kwargs have highest priority in the source chain.
- `load_config()` uses `os.chdir()` temporarily to make `TomlConfigSettingsSource` find the TOML file in the work directory. This is slightly hacky but pydantic-settings doesn't support specifying a custom TOML path at runtime — `toml_file` in `model_config` is class-level, not instance-level. The `chdir` is wrapped in a `try/finally` block.
- The `run` command now passes `cfg.max_attempts` to `generate_work_packages()` and `cfg.diamond_timeout` to `WorkerRunner`, using values from the config instead of hard-coded defaults.
- Kept the `backend` field as a simple `Literal` — it's not wired into the worker yet (Phase 2 will use it to choose between `local` mode and `ray` mode).

**Problems encountered**:
- `from __future__ import annotations` + Typer: Moving `Path` into a `TYPE_CHECKING` block broke Typer because it evaluates annotations at runtime to build CLI parameters. Got `NameError: name 'Path' is not defined` on all CLI commands. Fixed by keeping `Path` as a runtime import with `# noqa: TCH003`.
- `load_config()` initially tried to set an env var to redirect TOML loading — pydantic-settings `TomlConfigSettingsSource` ignores custom env vars and always reads from cwd. Switched to the `os.chdir()` approach.
- mypy strict rejected `**dict[str, object]` unpacked into `BaseSettings.__init__` because the init has specific typed kwargs. Added `# type: ignore[arg-type]` — the dict values are validated by Pydantic at runtime.

**Learnings**:
- Typer and `from __future__ import annotations` don't mix well with `TYPE_CHECKING` — Typer needs runtime access to type annotations for CLI parameter generation. Any type used in a `@app.command()` function signature must be a real runtime import.
- Pydantic Settings' TOML file discovery is cwd-relative and class-level. For runtime configuration of which file to read, temporary `chdir()` is the simplest workaround. An alternative would be a factory method that creates a subclass with a different `toml_file`, but that's over-engineered for this use case.

**Status**: Complete

---

### Task 2.1: Heartbeat mechanism — 2026-04-09

**What was done**:
- Made `FileSystemWorkStack.heartbeat()` thread-safe: catches `FileNotFoundError` when the package has been completed/failed by the main thread while the heartbeat is in flight. Logs a debug message instead of crashing.
- Created `HeartbeatSender` context manager in `worker/runner.py`: spawns a daemon thread that calls `work_stack.heartbeat(package_id)` every `interval` seconds. Stops cleanly on context exit via `threading.Event`. Catches and logs exceptions in the heartbeat call rather than crashing the worker.
- Integrated `HeartbeatSender` into `WorkerRunner.run()`: each claimed package is processed inside a `with HeartbeatSender(...)` block. The heartbeat starts after the package is claimed (state is RUNNING) and stops when processing completes/fails.
- Added `heartbeat_interval` parameter to `WorkerRunner.__init__` (default 30.0 seconds). Wired from `cfg.heartbeat_interval` in the CLI's `run` command.
- Exported `HeartbeatSender` from `worker/__init__.py`.
- `tests/test_heartbeat.py` — 7 tests across 3 classes: `heartbeat()` method correctness and thread-safety, `HeartbeatSender` lifecycle (periodic updates, clean shutdown, package completion during heartbeat, exception handling), and `WorkerRunner` integration with a slow mock DIAMOND.

**Decisions made**:
- `HeartbeatSender` uses `threading.Event.wait(timeout=interval)` for the sleep loop. This is cleaner than `time.sleep()` + checking a flag — the event wakes the thread immediately when `stop()` is called, rather than waiting for the current sleep to finish.
- The heartbeat thread is a daemon thread. If the main process crashes without calling `__exit__`, the daemon thread dies with it — no orphaned threads.
- On heartbeat exception (any `Exception`), the thread logs the error and exits. This prevents a broken heartbeat from retrying indefinitely. The package will eventually be reaped by the stale heartbeat reaper (Task 2.2).
- The heartbeat thread doesn't update any shared state with the main thread — it only writes to the filesystem (the work package JSON in `running/`). The main thread reads/moves the same file. Thread safety comes from the filesystem: if the file has been moved, `FileNotFoundError` is caught.

**Problems encountered**:
- None significant. The `threading.Event.wait(timeout=)` pattern worked cleanly on the first try. The integration test with the slow mock (0.3s DIAMOND delay, 0.05s heartbeat interval) reliably confirms the heartbeat fires during processing.

**Learnings**:
- `threading.Event.wait(timeout=interval)` is the idiomatic way to implement a stoppable periodic loop in Python. It combines sleeping and checking the stop signal in a single atomic call, and wakes immediately when the event is set.
- Context managers (`__enter__`/`__exit__`) are the right pattern for thread lifecycle — the caller doesn't need to remember to call `stop()`, and exception paths are handled automatically.

**Status**: Complete

---

### Task 2.2: Timeout reaper — 2026-04-09

**What was done**:
- Fixed `FileSystemWorkStack.reap_stale()`: packages with `heartbeat_at=None` are now treated as stale (previously skipped). Added `FileNotFoundError` handling around both the read and rename steps for thread safety. Improved error history messages to include the last heartbeat time and timeout value.
- Created `ReaperThread` context manager in `worker/runner.py`: daemon thread that calls `reap_stale(timeout_seconds)` every `interval` seconds. Same `Event.wait(timeout=)` pattern as `HeartbeatSender`. Catches and logs exceptions rather than crashing.
- Integrated `ReaperThread` into `WorkerRunner.run()`: the reaper wraps the entire worker claim loop, so it scans for stale packages from other dead workers while this worker is processing its own packages.
- Added `heartbeat_timeout` and `reaper_interval` parameters to `WorkerRunner.__init__`. Wired from config in the CLI.
- Exported `ReaperThread` from `worker/__init__.py`.
- `tests/test_reaper.py` — 13 tests across 4 classes: `reap_stale` basics (8 tests: stale/fresh/null-heartbeat/poisoned/empty/multiple), race conditions (1 test: file disappears mid-scan), `ReaperThread` lifecycle (3 tests: detection/shutdown/exception), and integration (1 test: dead worker → reaper reclaims → new worker processes).

**Decisions made**:
- `heartbeat_at=None` is now treated as stale, not skipped. A package in `running/` with no heartbeat means the worker never started heartbeating — it's dead. The error message distinguishes "heartbeat stale: last seen X" from "heartbeat never started".
- `ReaperThread` logs at WARNING on exception (not DEBUG) — a failing reaper is operationally significant, unlike a failed individual heartbeat.
- The integration test runs the reaper and worker sequentially rather than relying on the worker loop to wait for the reaper. The current worker loop exits immediately when `claim()` returns None — it doesn't poll. In a multi-worker scenario (Phase 2 with Ray), the reaper runs independently from any single worker's claim loop. The test validates the chain: stale heartbeat → reaper detects → pending → worker claims → processes.
- Race condition handling uses the write-then-unlink pattern: write the new state file first, then delete the old one. If the unlink fails (another thread moved it), the write may have created a duplicate — but this is benign because the package ID is unique and the next claim/reap will find it.

**Problems encountered**:
- The integration test initially failed because the worker loop exited before the reaper fired. The worker's `claim()` found nothing pending (the stale package was in `running/`, not `pending/`) and returned immediately. Fixed by running the reaper first to reclaim the package, then running the worker to process it.

**Learnings**:
- The reaper and worker loop have a timing dependency: the reaper must fire before the worker gives up. In a polling worker (Phase 2), this happens naturally because the worker retries `claim()`. In the current single-pass loop, the reaper needs to have already run. This is fine for Phase 1 where there's one worker, but Phase 2's multi-worker setup will need a polling claim loop with backoff.

**Status**: Complete

---

### Task 2.3: Multi-worker execution via multiprocessing — 2026-04-09

**What was done**:
- Replaced the single-pass worker loop with a **polling claim loop with exponential backoff** (0.5s → 1s → 2s → 5s max). Workers now retry `claim()` when the queue is empty, allowing them to pick up packages reaped from dead workers. Workers exit after `max_idle_time` seconds with no successful claims.
- Added `request_shutdown()` method to `WorkerRunner` using `threading.Event` for clean external shutdown.
- Added `run_worker_process()` module-level function as the entry point for worker subprocesses. Each subprocess creates its own structlog config, work stack connection, `DiamondWrapper`, and `WorkerRunner`.
- Updated CLI `run` command: when `--workers N > 1`, spawns N `multiprocessing.Process` instances, each running `run_worker_process()`. Waits for all to finish with `join()`, logs warnings on non-zero exits. Single worker mode (`--workers 1`) uses the existing in-process path with no multiprocessing overhead.
- `tests/test_multiworker.py` — 7 tests across 4 classes: polling loop (idle exit, process-and-exit, shutdown signal), multi-worker (all packages completed, no duplicates), dead-worker recovery (stale heartbeat → reaper → new worker processes), and actual process death via `SIGKILL` (multiprocessing.Process killed → reaper reclaims → recovery).

**Decisions made**:
- **Polling with exponential backoff** rather than a fixed interval. Short backoff (0.5s) right after a claim attempt means workers are responsive to new packages. Long backoff (capped at 5s) prevents busy-waiting on an empty queue. Backoff resets to 0.5s after any successful claim.
- **`max_idle_time` (default 30s)** as the exit condition rather than "pending == 0". The reaper may return packages to pending at any time — a worker that exits because pending is zero would miss reaped packages. Idle time is the right signal: "I've been polling for 30 seconds and nothing has appeared."
- **`multiprocessing.Process` (not Pool)**: each process is independent with its own lifecycle. A pool would farm tasks from a central queue, which conflicts with our work stack's pull-based claim model. Processes that crash are detected via non-zero exit codes.
- **Process arguments are all serialisable primitives** (`str` paths, `int`/`float` config values). No pickling of complex objects — each subprocess reconstructs its own `FileSystemWorkStack`, `DiamondWrapper`, etc. from the paths.
- The SIGKILL test uses a module-level function (`_slow_worker_target`) because macOS's `spawn` multiprocessing start method can't pickle local closures.

**Problems encountered**:
- `AttributeError: Can't pickle local object` — macOS uses the `spawn` multiprocessing start method by default, which pickles the target function and sends it to the child process. Local functions (defined inside a test method) can't be pickled. Moved the worker target to module level.
- Unit tests with timing-dependent behaviour (polling loops, idle timeouts, reaper intervals) take real clock time. The full unit test suite now takes ~5-6 minutes due to these tests. Timeouts are kept as short as possible without being flaky.

**Learnings**:
- The polling loop with backoff elegantly resolves the timing dependency between the reaper and worker identified in Task 2.2. Workers no longer exit on the first empty `claim()` — they keep polling, giving the reaper time to reclaim stale packages.
- macOS's `spawn` multiprocessing requires all process targets and arguments to be picklable. This is a stronger constraint than Linux's `fork` (where the child inherits the parent's memory). Designing `run_worker_process()` with only primitive arguments ensures cross-platform compatibility.
- `multiprocessing.Process` with independent work stacks (all accessing the same filesystem directory) gives true parallelism without shared-memory coordination. The filesystem's atomic `os.rename()` is the only synchronisation mechanism.

**Status**: Complete

---

### Task 2.4: Chaos testing — 2026-04-09

**What was done**:
- `tests/test_chaos.py` — 8 chaos tests across 7 test classes covering 7 failure scenarios:
  1. **Worker SIGKILL during alignment** (integration): Kill one of two workers mid-processing → surviving worker's reaper reclaims the dead worker's packages → all completed.
  2. **Simulated OOM — DIAMOND exit 137**: Mock fails twice with exit code 137, succeeds on 3rd → completed with 2 OOM entries in error_history. Also: always-OOM → poisoned.
  3. **Intermittent failures**: Mock alternates success/failure → all 4 packages eventually complete via retries.
  4. **Corrupt work package JSON**: Invalid JSON and wrong-schema JSON in `pending/` → worker skips them, moves to `poisoned/`, processes valid packages.
  5. **Result write failure (simulated disk full)**: Patch `pq.write_table` to raise `OSError` on first call → package is failed and retried, worker continues.
  6. **All workers die, then restart**: SIGKILL all workers → make heartbeats stale → spawn new workers → reapers reclaim → all packages completed.

**Bug found and fixed**:
- **Corrupt JSON crash in `claim()`**: `FileSystemWorkStack.claim()` renamed a file from `pending/` to `running/` then tried to parse it (`WorkPackage(**json.loads(...))`). If the JSON was corrupt (invalid syntax or wrong schema), the parse raised an unhandled exception and the corrupt file got stuck in `running/` forever — blocking the queue.
- **Fix**: Wrapped the JSON parse in `try/except`. On parse failure, the corrupt file is moved to `poisoned/` (so it's visible for investigation) and the worker continues to the next candidate. Logged as `corrupt_work_package` at ERROR level.

**Decisions made**:
- Integration chaos tests (SIGKILL scenarios) use module-level target functions for macOS `spawn` compatibility. They manually make heartbeats stale after kill rather than waiting for real staleness — this makes tests faster and deterministic.
- Single-process chaos tests (OOM, intermittent failures, corrupt JSON, write failures) use mocks — no multiprocessing needed, much faster.
- The `_wait_for()` helper polls `stack.status()` with a timeout rather than using fixed `time.sleep()` — this makes tests as fast as possible while still reliable.

**Fault tolerance verified under 7 failure scenarios**:
- Worker process death (SIGKILL) during processing
- DIAMOND OOM (exit 137) with retry and poison
- Intermittent DIAMOND failures with retry
- Corrupt work package JSON in the queue
- Disk write failures during result output
- Full cluster death and restart
- Wrong-schema work package files

**Status**: Complete

---

### Task 2.5: Ray integration — 2026-04-10

**What was done**:
- `src/distributed_alignment/worker/ray_actor.py` — `AlignmentWorker` Ray actor wrapped in `create_alignment_actor()` factory and `run_ray_workers()` orchestrator. The actor reconstructs all dependencies (logging, work stack, DiamondWrapper, WorkerRunner) from a plain-dict config inside the Ray process. `_try_import_ray()` provides a clear error message if Ray is not installed.
- Updated `src/distributed_alignment/cli.py` with `--backend` flag and three dispatch paths: `_run_single_worker()` (1 worker, in-process), `_run_multiprocess_backend()` (N workers, multiprocessing), `_run_ray_backend()` (N workers, Ray actors).
- Updated `pyproject.toml`: Ray as optional dependency (`[project.optional-dependencies] ray = ["ray[default]>=2.9"]`), mypy override for `ray.*`.
- `tests/test_ray_worker.py` — 7 tests: import handling (success/failure), backend flag, and Ray integration tests (basic functionality, concurrent execution, error handling). Integration tests skip on paths with spaces (iCloud Drive — known Ray limitation).

**Decisions made**:
- **Ray is an optional dependency** — installed via `uv add 'distributed-alignment[ray]'`. The CLI handles missing ray gracefully with a clear error message. All existing tests pass without ray installed.
- **Actor config is a plain dict**, not a Pydantic model. Ray serialises actor arguments, and plain dicts are universally serialisable. The actor reconstructs typed objects inside its process.
- **`RAY_RUNTIME_ENV_HOOK=""` disables Ray's uv hook** which conflicts with `uv run` invocations. The `runtime_env` sets `PYTHONPATH=src` so Ray workers can find the project modules.
- **Ray integration tests skip on iCloud paths** (`_HAS_SPACE_IN_PATH` check). Ray's working directory packaging fails when the path contains spaces. This affects local development on iCloud Drive but not Docker, CI, or any standard filesystem path.
- The **Ray backend produces identical results** to the local/multiprocessing backends — same WorkerRunner, same HeartbeatSender, same ReaperThread. Ray only manages process lifecycle.

**Problems encountered**:
- Ray packages the working directory by default and creates fresh venvs for workers. On iCloud paths with spaces, this fails silently (workers hang during file packaging). Disabling the uv runtime env hook and setting `runtime_env` with `PYTHONPATH` was necessary.
- Even with the hook disabled, `ray.init()` hangs on the iCloud path. This is a fundamental limitation of Ray's file packaging with space-containing paths. The solution: skip Ray integration tests on such paths, rely on Docker/CI for Ray testing.
- `AlignmentWorker.remote()` call needs `# type: ignore[attr-defined]` — the `@ray.remote` decorator adds `.remote()` at runtime, which mypy can't see.

**Learnings**:
- Ray's `runtime_env` is powerful for cluster deployments but complex for local development, especially with non-standard paths. For local single-machine use, `multiprocessing` is simpler and more reliable.
- Making Ray optional via `[project.optional-dependencies]` is the right pattern — it keeps the core pipeline lightweight and only pulls in Ray's large dependency tree when explicitly needed.
- The `_try_import_ray()` pattern (lazy import with clear error) is cleaner than a top-level import that crashes on `ImportError`.

**Status**: Complete

---

### Ray Docker testing fix — 2026-04-14

**What was done**:
- Updated `Dockerfile.dev` to install Ray via `uv pip install "ray[default]>=2.9"` (the `--extra ray` flag didn't work with `package = false`).
- Fixed `ray_actor.py`: changed `os.environ["RAY_RUNTIME_ENV_HOOK"] = ""` to `os.environ.pop("RAY_RUNTIME_ENV_HOOK", None)` — setting to empty string caused `ValueError`, needs to be deleted.
- Updated `docker-compose.yml` to use `.venv/bin/python -m pytest` instead of `uv run pytest`. The `uv run` command sets `RAY_RUNTIME_ENV_HOOK` which interferes with Ray worker process startup and can't be reliably removed at runtime.
- Added `_clear_ray_hook` autouse fixture in `test_ray_worker.py`.
- Fixed `test_ray_worker.py` integration tests to use real DIAMOND (mocks can't be sent to Ray actor processes — they're separate processes that can't receive non-picklable objects).

**Docker test results**: 205 tests total — 204 passed + 1 timing flake (passes on re-run). All 7 Ray tests pass with real DIAMOND in Docker, including:
- 2 actors processing 4 packages → all completed
- Concurrent execution verification
- Actor error handling with bad config

**Root cause of previous hangs**: `uv run` injects `RAY_RUNTIME_ENV_HOOK` env var. Ray's worker subprocesses inherit this, causing them to try to use uv's runtime env hook which then fails. The fix: bypass `uv run` for Ray tests by using the venv's Python directly.

- Added `make test-docker` target — builds the Docker image and runs the full suite (DIAMOND + Ray) in one command. Also added `make docker-build` as a standalone build target.
- Updated `README.md` with current test commands, multi-worker/Ray usage, and Phase 2 status.

**Status**: Complete

---

### Task 2.6: Prometheus metrics — 2026-04-15

**What was done**:
- `src/distributed_alignment/observability/metrics.py` — 7 Prometheus metrics matching the TDD spec:
  - `da_packages_total` (Gauge by state): work packages per state
  - `da_package_duration_seconds` (Histogram): time to process one package
  - `da_sequences_processed` (Counter): total sequences aligned
  - `da_hits_found` (Counter): total alignment hits
  - `da_worker_count` (Gauge): active workers
  - `da_errors` (Counter by error_type): errors categorised as oom/timeout/diamond_error/write_error/missing_chunk/exception
  - `da_diamond_exit_code` (Counter by exit_code): DIAMOND exit codes
- Helper functions: `start_metrics_server(port)`, `record_package_completed()`, `record_package_failed()`, `record_diamond_result()`, `update_package_states()`.
- Updated `src/distributed_alignment/observability/__init__.py` to export all metrics functions.
- Integrated metrics into `WorkerRunner`:
  - `run()`: increments/decrements `da_worker_count`, calls `update_package_states()` on each poll cycle.
  - `_process_package()`: times each package, calls `record_package_completed()` on success.
  - `_run_alignment()`: calls `record_diamond_result()` after every DIAMOND execution, `record_package_failed()` with categorised error types on failure.
- Changed `_run_alignment()` return type from `Path | None` to `tuple[Path | None, int]` to return hit count alongside the result path.
- Added `prometheus-client>=0.20` to runtime dependencies.
- `tests/test_metrics.py` — 13 tests across 6 classes: metric definitions, record helpers (histogram/counters/gauges), metrics server (HTTP endpoint + port-busy handling), and WorkerRunner integration.

**Decisions made**:
- Counter names don't include `_total` suffix (prometheus_client adds it automatically). Named `da_sequences_processed` not `da_sequences_processed_total` — the exposed metric is `da_sequences_processed_total` per Prometheus convention.
- Metrics are emitted from `WorkerRunner` (not from `FileSystemWorkStack`) because the worker has timing information and knows the semantic context (was this a success? what type of error?).
- `update_package_states()` is called on every poll cycle iteration, not just on state changes. This is cheap (reads a few directory listings) and ensures the gauge always reflects current state, including changes from the reaper.
- `start_metrics_server()` catches `OSError` for port-busy — in multi-worker mode, only the first worker's server succeeds. Others silently skip (metrics are still tracked in-process, just not exposed via HTTP). The CLI or an orchestrator would handle aggregation.
- Multiprocess metrics sharing is deferred — each process has its own prometheus_client registry. For production, a push gateway or prometheus_client's multiprocess mode would be needed. Documented as a known limitation.

**Problems encountered**:
- prometheus_client counter naming: defining `Counter("da_sequences_processed_total", ...)` creates a sample named `da_sequences_processed_total_total` (double `_total`). Fixed by dropping `_total` from the counter definition name — prometheus_client adds the suffix automatically.

**Learnings**:
- prometheus_client's `REGISTRY.get_sample_value()` is the right way to assert metric values in tests — no HTTP server needed, reads directly from the in-process registry.
- Counter sample names always end in `_total` regardless of whether the Counter name includes it. Gauge and Histogram names are used as-is.

**Status**: Complete

---

### Task 2.7: Grafana dashboard and metrics backend abstraction — 2026-04-15

**What was done**:

**Part 1 — Dual metrics backend**:
- Refactored `observability/metrics.py` into a dual-backend architecture: `PrometheusMetrics` (local/multiprocessing) and `RayMetrics` (Ray actors). Both implement the same interface: `observe_duration()`, `inc_sequences()`, `inc_hits()`, `inc_worker()`, `dec_worker()`, `inc_error()`, `inc_diamond_exit()`, `set_package_state()`.
- `get_metrics()` auto-detects the backend: uses `RayMetrics` when `ray.is_initialized()`, `PrometheusMetrics` otherwise. Returns a singleton.
- `reset_metrics()` clears the singleton for testing.
- prometheus_client metrics are now module-level objects (registered once) with `PrometheusMetrics` holding references. Prevents "Duplicated timeseries" errors when `reset_metrics()` is called between tests.
- `start_metrics_server()` is a no-op for the Ray backend (Ray exposes metrics via its own endpoint).
- Added `inc_worker()`/`dec_worker()` helper functions used by `WorkerRunner.run()`.
- `RayMetrics` uses `ray.util.metrics` with tag-based labels instead of prometheus_client's positional labels.

**Part 2 — Grafana dashboard and monitoring stack**:
- `observability/prometheus.yml` — Prometheus config scraping `host.docker.internal:9090` every 5s.
- `observability/grafana/provisioning/datasources.yml` — auto-provisions Prometheus as default datasource.
- `observability/grafana/provisioning/dashboards.yml` — auto-provisions dashboard from JSON.
- `observability/grafana/dashboards/distributed-alignment.json` — 10-panel dashboard:
  - Row 1 (Overview): Pipeline Progress gauge, Packages by State stats, Active Workers, Total Hits
  - Row 2 (Performance): Package Duration p50/p95/p99 timeseries, Throughput (hits/sec, sequences/sec)
  - Row 3 (Errors & Cost): Errors by Type timeseries, DIAMOND Exit Codes bar gauge, Estimated Cost stat with configurable `$cost_per_cpu_hour` variable (default 0.0464)
  - Auto-refresh 5s, 15-minute time range, anonymous access
- Updated `docker-compose.yml` with `prometheus` and `grafana` services alongside the existing `dev` service.
- Updated `README.md` with Monitoring section.

**Tests**: 16 metrics tests — 3 new backend tests (auto-detection, singleton, reset) plus 13 existing tests updated for the new architecture.

**Decisions made**:
- prometheus_client metrics are module-level singletons, not instance attributes. This prevents duplicate registration errors when `reset_metrics()` creates a new `PrometheusMetrics` instance. The `PrometheusMetrics` class just holds references.
- `RayMetrics.inc_worker()`/`dec_worker()` use `getattr(self.worker_count, "_value", 0)` for manual tracking since `ray.util.metrics.Gauge` doesn't support `.inc()`/`.dec()` — only `.set()`.
- Grafana uses anonymous access with Viewer role — no login needed for the dashboard. Admin password is `admin` for configuration changes.
- Prometheus scrapes `host.docker.internal:9090` to reach the pipeline running on the host machine from inside Docker.

**Status**: Complete

---

### Task 2.8: Docker packaging (production-ready) — 2026-04-15

**What was done**:
- `Dockerfile` — production multi-stage build:
  - Stage 1 (build): installs uv, syncs runtime deps only (`--no-dev`), copies `src/`.
  - Stage 2 (runtime): python:3.11-slim + DIAMOND via miniforge/bioconda (arm64/x86_64), copies venv + src from build stage, non-root user (`da`), health check, `ENTRYPOINT ["python", "-m", "distributed_alignment"]`.
- Updated `docker-compose.yml` with full production stack:
  - `worker` service: builds from production Dockerfile, `restart: on-failure`, `deploy.replicas: 2`, shared volume for work data.
  - `ingest` service: one-off (`profiles: ["setup"]`), mounts `./data/input` read-only.
  - `prometheus` and `grafana`: unchanged from Task 2.7 but Prometheus config updated to scrape `worker:9090` (Docker service name) in addition to host fallback.
  - `dev` service: unchanged, builds from Dockerfile.dev.
  - `shared-data` named volume shared across all services.
- Updated `observability/prometheus.yml` to scrape three targets: Docker workers (`worker:9090`), host pipeline (`host.docker.internal:9090`), and Ray dashboard (`host.docker.internal:8265`).
- Updated `.dockerignore` with additional exclusions (data/, demo/, notebooks/, *.dmnd).
- Updated `README.md` with complete Docker workflow: build → ingest → workers → monitoring → scale → status.

**Decisions made**:
- Multi-stage build separates build deps (uv) from runtime. The final image doesn't contain uv or dev dependencies — only Python, DIAMOND, and the project code.
- Each Docker worker container runs with `--workers 1`. Scaling is via `--scale worker=N`, not `--workers N`. This matches the container orchestration model (each container = one worker, Docker/K8s manages replicas).
- Non-root user (`da`) for security. The `/data` directory is owned by this user.
- `restart: on-failure` provides container-level fault tolerance: if a worker crashes (segfault, OOM kill), Docker restarts it. The application-level reaper handles the abandoned packages.
- Ingest is a `profiles: ["setup"]` service — only runs when explicitly invoked, not on `docker-compose up`.
- Kept `Dockerfile.dev` as-is — dev needs pytest, mypy, ruff, ray, and source mounting. Production doesn't.

**Status**: Complete

---

### Metrics endpoint wiring and Grafana fixes — 2026-04-15

**What was done**:
- **Wired `start_metrics_server()` into `WorkerRunner.run()`**: Workers now expose a Prometheus HTTP endpoint on startup. Previously, metrics were tracked in-process but never exposed — Prometheus had nothing to scrape. Added `metrics_port` parameter to `WorkerRunner.__init__`, `run_worker_process()`, and the Ray actor config, threaded through from `cfg.metrics_port` in the CLI.
- **Fixed Grafana provisioning directory structure**: Grafana expects provisioning config in `provisioning/datasources/` and `provisioning/dashboards/` subdirectories. Moved `datasources.yml` and `dashboards.yml` into their respective subdirectories. Dashboard JSON mounted separately to `/var/lib/grafana/dashboards` (referenced by the provider config).
- **Fixed `RayMetrics.inc(0)` crash**: `ray.util.metrics.Counter.inc()` rejects `value=0` (unlike prometheus_client). Added `if n > 0` guards in `RayMetrics.inc_sequences()` and `RayMetrics.inc_hits()`. This was causing `ValueError: value must be >0, got 0` in Ray integration tests.
- **Demonstrated full live pipeline**: Re-ingested with `--chunk-size 10` producing 10 query × 50 reference = 500 work packages. 2 Docker workers processed all 500 packages while Grafana dashboard showed real-time progress: packages by state, active workers, total hits, package duration percentiles, throughput, DIAMOND exit codes, and estimated cost — all updating live.

**End-to-end verified workflow**:
```
docker-compose run --rm ingest                          # 500 work packages
docker-compose up -d prometheus grafana                 # monitoring
docker-compose up -d worker                             # 2 workers
open http://localhost:3000/d/da-pipeline                # live dashboard
```

All 500 packages completed, 0 poisoned, 248 successful DIAMOND alignments, p50 duration ~500ms, estimated cost $0.000110.

**Known limitation**: The "Pipeline Progress" gauge shows "No data" — the PromQL division query requires all four state labels to exist simultaneously in the same scrape. Minor dashboard polish for a future fix.

**Status**: Complete

---

### Task 2.9: GitHub Actions CI/CD — 2026-04-15

**What was done**:
- `.github/workflows/ci.yml` — 4 CI jobs:
  1. **quality** (every push/PR): `ruff check`, `ruff format --check`, `mypy --strict`. ~1 min.
  2. **unit-tests** (every push/PR): `pytest -m "not integration"` with coverage report uploaded as artifact. ~10 min (includes timing-dependent multiworker tests).
  3. **integration-tests** (main + PRs to main, after quality + unit-tests pass): installs DIAMOND v2.1.10 Linux binary, runs `pytest -m "integration"`. ~15 min.
  4. **docker-build** (every push/PR, parallel): builds production Dockerfile, verifies `--help` works.
- `.github/dependabot.yml` — weekly updates for GitHub Actions versions.
- Applied `ruff format` across entire codebase (24 files reformatted) to ensure `ruff format --check` passes in CI.
- Added CI badge to README.md.

**Decisions made**:
- **quality and unit-tests run in parallel** — both fast, independent. integration-tests depends on both (`needs: [quality, unit-tests]`) to avoid wasting CI minutes on heavy DIAMOND tests if basic checks fail.
- **docker-build runs in parallel** with everything — it's independent and catches Dockerfile issues early.
- **DIAMOND installed via direct binary download** in integration-tests (not conda) — faster and simpler for CI. The `diamond-linux64.tar.gz` from GitHub releases works on ubuntu-latest's x86_64.
- **uv cached** via `astral-sh/setup-uv@v4` with `enable-cache: true` for faster dependency installs across runs.
- **PYTHONPATH=src** set at job-level `env` so all steps inherit it (matches the `package = false` project setup).
- **Timeout-minutes** on each job: quality 5, unit-tests 15, integration-tests 15, docker-build 10. Prevents hung jobs from burning CI minutes.
- Integration-tests run on `github.ref == 'refs/heads/main' || github.event_name == 'pull_request'` — runs on PRs to main but not on every branch push.

**Also done**: Applied `ruff format` to normalise code style across all 42 Python files. No functional changes — only formatting (line wrapping, quote style, trailing commas).

**Status**: Complete

---

## Phase 3: Enrichment & Features

### Dataset assembly (Phase 3 prerequisite) — 2026-04-16

**What was done**:
- `docs/scientific-context.md` — ~1,050-word document explaining metagenomics, the alignment workflow, the dark matter problem, and how the pipeline contributes. Written for software engineers, rigorous enough for bioinformaticians.
- `scripts/download_test_fixtures.py` — downloads taxonomically diverse Swiss-Prot fixtures via UniProt REST API:
  - **Query set** (500 sequences, 9 organisms): simulates a soil metagenomic community. Headers anonymised (accession + protein name only, no organism info). Organisms span Bacteria, Archaea, and Eukarya across 9 phyla.
  - **Reference set** (2,650 sequences, 13 organisms): broad taxonomic diversity including prokaryotes, eukaryotes, and extremophiles.
  - **Ground truth** (`ground_truth.json`): maps each query accession to its true organism, taxon ID, and phylum for validation.
- `scripts/download_metagenome.py` — Tier 2: downloads real soil metagenome proteins from MGnify + full Swiss-Prot (~300MB to `data/metagenome/`).
- `scripts/download_stress_test.py` — Tier 3: downloads multiple MGnify analyses + UniRef50 (~1-5GB to `data/stress_test/`).
- `data/README.md` — explains the tier structure and download commands.
- Updated `tests/conftest.py` with `metagenome_queries_path` and `diverse_reference_path` fixtures (skip-if-missing pattern).

**Tier 1 fixtures downloaded and committed**:
- `tests/fixtures/metagenome_queries.fasta` — 500 sequences, ~227KB
- `tests/fixtures/diverse_reference.fasta` — 2,650 sequences, ~1.6MB
- `tests/fixtures/ground_truth.json` — 500 entries, ~60KB
- Total: ~1.9MB (within 2MB target)

**Dataset design rationale**:
- Query organisms chosen to simulate a realistic soil metagenome: *Bacillus*, *Streptomyces*, *Pseudomonas* (common soil bacteria), *Methanosarcina* (archaeal methanogens), *Nostoc* (cyanobacteria), *Thermus*/*Sulfolobus* (extremophiles), *Rhodopirellula* (undersampled planctomycete). This provides genuine taxonomic diversity for enrichment testing.
- Headers anonymised to simulate the real metagenomics workflow: you don't know which organism a protein came from until you align it. Ground truth enables validation.
- Reference set includes overlapping organisms with queries (e.g. *B. subtilis*, *S. cerevisiae*) to ensure DIAMOND produces real hits, plus organisms not in the query set to test taxonomic enrichment breadth.

**Status**: Complete

---

### Scientific context update + pathogen surveillance dataset — 2026-04-16

**What was done**:

**Part 1 — Scientific context rewrite**:
- Rewrote Section 3 (distributed alignment) to clearly explain queries vs references, what chunking actually does (computational parallelisation, not biological filtering), and that every query is compared against every reference.
- Added pathogen surveillance subsection to Section 5 — connecting the same pipeline architecture to clinical metagenomics (*C. difficile*, *S. aureus*, diagnostic classifiers).

**Part 2 — Pathogen dataset extension**:
- Added `PATHOGEN_ORGANISMS` to `download_test_fixtures.py`: *C. difficile* 630, *S. enterica* Typhimurium, *S. aureus* NCTC 8325, *K. pneumoniae* HS11286 (400 sequences total).
- Created `tests/fixtures/pathogen_reference.fasta` (400 sequences, ~217KB) as a separate file — keeps existing diverse_reference.fasta stable for existing tests.
- Made download script idempotent — skips existing fixtures, only downloads what's missing.
- Added clinical gut metagenome download to `scripts/download_metagenome.py` — MGnify HMP gut study, saves to `data/clinical/gut_metagenome.fasta`.
- Updated `data/README.md` with two analysis scenarios (biodiscovery vs pathogen surveillance) and ground truth explanation.
- Added `pathogen_reference_path` fixture to `tests/conftest.py` (skip-if-missing pattern).

**Status**: Complete

---

### Task 3.0: NCBI taxonomy loader — 2026-04-16

**What was done**: Built the taxonomy module that parses NCBI taxonomy dumps into a queryable DuckDB-backed database with lineage lookups.

Files created:
- `src/distributed_alignment/taxonomy/ncbi_loader.py` — `TaxonomyDB` class: parses `nodes.dmp`, `names.dmp`, and `accession2taxid` into DuckDB tables, loads the tree into Python dicts for fast lineage walking, provides `get_lineage()`, `get_taxon_id_for_accession()`, `get_lineage_for_accession()`, and `batch_lineage()` methods.
- `src/distributed_alignment/taxonomy/__init__.py` — Exports `TaxonomyDB`.
- `tests/fixtures/taxonomy/nodes.dmp` — 171 hand-curated NCBI taxonomy nodes covering all organisms in the test data (E. coli through Homo sapiens, all query, reference, and pathogen organisms) with complete lineage paths from each species to root.
- `tests/fixtures/taxonomy/names.dmp` — Scientific names (and some synonyms/common names for testing the filter) for all 171 nodes.
- `tests/fixtures/taxonomy/accession2taxid.tsv` — 38 accessions from `diverse_reference.fasta` and `pathogen_reference.fasta` mapped to their correct taxon IDs.
- `scripts/download_taxonomy.sh` — Idempotent shell script to download NCBI taxdump + `prot.accession2taxid.gz` to `data/taxonomy/`.
- `tests/test_taxonomy_loader.py` — 48 tests across 10 test classes.

**Decisions made**:
- Tree walking done in Python dicts rather than DuckDB recursive CTEs. The NCBI tree has ~2.5M nodes but as a Python dict of `{int: int}` it's only ~100MB — fast enough for interactive lookups and avoids complex SQL. DuckDB is only used for accession lookups where an indexed scan matters at scale.
- NCBI's "superkingdom" rank maps to the "kingdom" key in lineage output (Bacteria, Archaea, Eukaryota). The NCBI "kingdom" rank (Metazoa, Fungi, Viridiplantae) is traversed during the walk but not captured — it's a eukaryote-only rank that would be confusing in the output alongside superkingdom.
- `from_ncbi_dump` is idempotent: if the DuckDB file exists, it's loaded directly without re-parsing. This is the cache mechanism — parse once (~seconds for fixtures, ~minutes for full NCBI), then load instantly on subsequent runs.
- Fixture `accession2taxid.tsv` uses the same 4-column format as NCBI's `prot.accession2taxid` (accession, accession.version, taxid, gi) so the parser works identically on real and fixture data.

**Problems encountered**:
- NCBI `.dmp` files terminate each line with `\t|` (tab-pipe). When splitting on `\t|\t`, the last field retains a trailing `\t|` that isn't stripped by `.strip()` alone (since `|` isn't whitespace). Initial parse produced ranks like `"no rank\t|"` instead of `"no rank"`. Fixed by `rstrip("|")` on the full line before splitting.
- Building the fixture nodes.dmp required tracing complete lineage paths from every test species to root. For Homo sapiens alone, this is 29 intermediate nodes (through Mammalia, Chordata, Metazoa, Opisthokonta etc.). Missing a single intermediate node breaks the tree walk. Verified all paths manually against NCBI taxonomy browser structure.

**Learnings**:
- NCBI taxonomy has many "no rank" and "clade" intermediate nodes between the standard Linnaean ranks. The lineage walker needs to traverse all of these but only capture names at the 7 ranks of interest. This is why a dict-based walk is simpler than a SQL approach — the SQL would need to filter ranks during the recursive CTE.
- The tab-pipe-tab format in `.dmp` files is deceptively tricky. The trailing delimiter pattern means naive splitting produces dirty field values. Stripping the line-level terminator before splitting is cleaner than per-field cleanup.

**Status**: Complete

---

### Task 3.1: Taxonomic enrichment — 2026-04-16

**What was done**: Built the enrichment pipeline that annotates merged alignment results with NCBI taxonomy lineage data and computes per-query taxonomic profiles.

Files created:
- `src/distributed_alignment/taxonomy/enricher.py` — Two main functions: `enrich_results()` reads merged Parquet, extracts accessions from `subject_id`, batch-looks up lineages via `TaxonomyDB`, appends 8 taxonomy columns (taxon_id, species, genus, family, order, class, phylum, kingdom), writes enriched Parquet. `compute_taxonomic_profiles()` uses DuckDB SQL to aggregate: GROUP BY query_id, phylum → hit_count, mean_percent_identity, best_evalue per phylum per query. Also defines `ENRICHED_SCHEMA` and `PROFILE_SCHEMA` as PyArrow schemas.
- `tests/test_enricher.py` — 17 tests across 5 classes: accession extraction (5 format variants), basic enrichment (E. coli annotation, multi-phyla, unmapped preservation, plain accession, empty input), schema validation (taxonomy columns present, originals preserved, taxon_id type), taxonomic profiles (3-phyla distribution, statistics correctness, multi-query, schema).
- Updated `src/distributed_alignment/taxonomy/__init__.py` — exports `enrich_results` and `compute_taxonomic_profiles`.

**Decisions made**:
- Used PyArrow column-append rather than DuckDB JOIN for adding taxonomy columns to the merged table. The enrichment needs per-row accession extraction (Python string split), so building Python arrays and appending them to the Arrow table is simpler than constructing a DuckDB temp table and joining. The accession lookups are deduplicated first (unique subject_ids → unique accessions), so the Python loop only does dict lookups per row — fast even for millions of hits.
- Used DuckDB for the taxonomic profile aggregation — GROUP BY with AVG/MIN/COUNT is exactly what DuckDB excels at, and it reads the enriched Parquet directly without loading into Python.
- Unmapped accessions get `"unknown"` for all taxonomy string columns and null for `taxon_id`. This preserves the hit row (not dropped) and makes downstream queries simple — filter `WHERE phylum != 'unknown'` to exclude unmapped, or `WHERE taxon_id IS NULL` to find them.
- DuckDB's `.arrow()` returns a `RecordBatchReader`, not a `Table` — need `.read_all()` before passing to `pq.write_table()`. Same issue that was encountered in the merger.

**Problems encountered**:
- DuckDB `.arrow()` returns `RecordBatchReader` not `pyarrow.Table`. Calling `pq.write_table()` on a RecordBatchReader fails with `TypeError: expected pyarrow.lib.Table, got pyarrow.lib.RecordBatchReader`. Fixed by chaining `.arrow().read_all()`.

**Learnings**:
- The accession deduplication step is the key performance optimization. Real alignment results may have millions of rows but only thousands of unique reference proteins. Looking up thousands of accessions is instant; looking up millions would be slow. The pattern is: deduplicate → batch lookup → build column arrays via dict lookups per row.
- PyArrow's `table.append_column()` returns a new table (immutable), so the pattern is `table = table.append_column(...)` in a loop. This is efficient because PyArrow uses zero-copy for the existing columns.

**Status**: Complete

---

### Task 3.2: Alignment feature extraction (Stream A) — 2026-04-16

**What was done**: Built the per-query feature extractor that computes alignment statistics, taxonomic diversity metrics, and coverage from enriched alignment results.

Files created:
- `src/distributed_alignment/features/alignment_features.py` — `extract_alignment_features()` computes 10 features per query sequence via a single DuckDB CTE query: hit_count, mean/max percent_identity, mean_evalue_log10, mean/std alignment_length, best_hit_query_coverage, taxonomic_entropy (Shannon), num_phyla, num_kingdoms. Uses LEFT JOIN against the full query list from chunk Parquet files to include zero-hit queries. Adds metadata columns (feature_version, run_id, created_at). Defines `FEATURE_SCHEMA` as PyArrow schema.
- `tests/test_alignment_features.py` — 16 tests across 5 classes: basic features (hit count, identity, alignment length, evalue log10), taxonomic entropy (single phylum = 0, uniform 3 phyla = log2(3), skewed 3:1 ≈ 0.811), zero-hit queries (included with hit_count=0, null float features), coverage and edge cases (best_hit_query_coverage, evalue=0 sentinel, single-hit std is null, unknown phyla excluded), determinism and schema validation.
- Updated `src/distributed_alignment/features/__init__.py` — exports `extract_alignment_features`.

**Decisions made**:
- All feature computation in a single DuckDB CTE query. Five CTEs: `agg` (basic aggregations), `best` (best-hit alignment length for coverage), `phylum_dist` + `query_totals` + `entropy` (Shannon entropy from phylum distribution). This avoids multiple passes over the data and leverages DuckDB's optimizer.
- Zero-hit queries get hit_count=0 and num_phyla/num_kingdoms=0 via COALESCE, but float features (mean_identity, entropy, coverage, etc.) are NULL. NULL correctly distinguishes "no data" from computed values like 0 (which means "all hits in one phylum" for entropy). Downstream pandas reads convert NULLs to NaN automatically.
- Query lengths are read from `chunks/queries/*.parquet` (the chunk files from ingestion), not from the enriched results. This ensures the feature output includes all query sequences even if they have zero alignment hits.
- evalue=0 (perfect matches) handled with `CASE WHEN evalue > 0 THEN LOG10(evalue) ELSE -999.0 END` sentinel value. This avoids -inf from log10(0) while clearly marking these values as extreme. The -999 is distinguishable from any real log10(evalue).
- `STDDEV_SAMP` (not STDDEV_POP) for std_alignment_length — sample standard deviation is undefined for n=1, so single-hit queries get NULL. This is correct behaviour, not a bug.

**Problems encountered**:
- DuckDB `.arrow()` returns `RecordBatchReader` not `Table` — same issue as Task 3.1. Chained `.arrow().read_all()`.
- Ruff's line length limit (88) conflicts with readable SQL formatting (aligned AS clauses). Reformatted SQL to use line-broken `AS` clauses rather than right-padded alignment.
- `datetime.timezone.utc` flagged by ruff UP017 — should use `datetime.UTC` alias (Python 3.11+).

**Learnings**:
- DuckDB CTEs are powerful for complex multi-step feature engineering. The entropy calculation requires computing phylum distributions, proportions, and the Shannon formula — all expressible as CTEs without materialising intermediate results in Python.
- LEFT JOIN against a master query list is essential for producing a complete feature table. Without it, queries with no alignment hits would be silently dropped, creating a biased feature matrix.

**Status**: Complete

---

### Task 3.3: k-mer frequency features (Stream B) — 2026-04-17

**What was done**: Built the k-mer feature extractor that computes 8,000-dimensional amino-acid 3-mer frequency vectors for every query sequence.

Files created:
- `src/distributed_alignment/features/kmer_features.py` — Module-level constants: `AMINO_ACIDS` (20 standard), `KMER_VOCABULARY` (8,000 3-mers via `itertools.product`, stable ordering), `KMER_SCHEMA` (PyArrow schema with `pa.list_(pa.float32(), 8000)` fixed-size list). Core function `_kmer_frequencies()` does overlapping 3-mer counting with non-standard residue filtering and frequency normalisation. Public function `extract_kmer_features()` reads all query chunks, computes per-sequence vectors, and writes Parquet with metadata columns.
- `tests/test_kmer_features.py` — 21 tests across 7 classes: vocabulary (length, first/last, character set), hand-verified counts (AAAA, ACACACAC, AAACCC with exact frequency assertions), summation invariant (4 parametrised sequences), edge cases (too short, empty, non-standard residues, all non-standard, lowercase), schema/metadata, multi-chunk integration, determinism.
- Updated `src/distributed_alignment/features/__init__.py` — exports `extract_kmer_features`, `KMER_SCHEMA`, `KMER_VOCABULARY`.

**Decisions made**:
- Fixed-size list column (`pa.list_(pa.float32(), 8000)`) rather than 8,000 individual float columns. More compact in Parquet, easier to slice in downstream code, and matches how ML libraries expect dense feature vectors. The alternative (one column per k-mer) would make the schema unwieldy and slow to read column-by-column.
- `float32` rather than `float64` — frequencies are small numbers (count/total) that don't need double precision. Halves the storage for the 8,000-element vectors. This is standard practice for ML feature matrices.
- Non-standard residue handling: skip any 3-mer containing a character outside the 20 standard amino acids. Don't count it toward the denominator. This means a sequence like `AAAXAAA` produces a clean vector for the valid portions rather than injecting noise. Sequences with zero valid 3-mers get an all-zero vector (no error).
- `KMER_VOCABULARY` built via `itertools.product(AMINO_ACIDS, repeat=3)` with alphabetically ordered amino acids. This gives a deterministic, stable ordering that tests depend on. Module-level constant, computed once at import time.

**Problems encountered**:
- `.gitignore` had `features/` (unanchored) which matched `src/distributed_alignment/features/` — silently preventing git from tracking the features source code. Fixed by changing to `/features/` (anchored to repo root). Debugged with `git check-ignore -v <path>`.

**Learnings**:
- PyArrow's `FixedSizeListArray.from_arrays()` takes a flat array of all values plus a `list_size` parameter. So for N sequences × 8,000 elements, you pass a flat array of N×8,000 floats. The flat-then-reshape pattern: `[v for vec in freq_vectors for v in vec]`.
- Gitignore patterns without a leading `/` match at any directory depth. This is a recurring trap when project-specific output directories share names with source packages.

**Status**: Complete

---

### Task 3.4: ESM-2 embedding integration (Stream C) — 2026-04-17

**What was done**: Added ESM-2 protein language model embeddings as an optional third feature stream, cleanly separated into a loader module (no torch dependency) and a standalone compute script (requires torch + fair-esm).

Files created:
- `src/distributed_alignment/features/embedding_features.py` — Loader module: `EMBEDDING_SCHEMA` (PyArrow schema with `pa.list_(pa.float32(), 320)` fixed-size list), `EMBEDDING_DIM = 320`, `MODEL_NAME = "esm2_t6_8M_UR50D"`, `load_embeddings()` that reads and validates a pre-computed Parquet. **No torch or esm imports** — pure PyArrow, importable in the default environment.
- `scripts/compute_embeddings.py` — Standalone Typer CLI: reads FASTA via the project's existing parser, loads ESM-2 (`esm2_t6_8M_UR50D`, 8M params, 6 layers), processes in batches, mean-pools final-layer representations over residue positions (excluding BOS/EOS), writes Parquet conforming to `EMBEDDING_SCHEMA`. Guarded `import torch / esm` with a clear error message pointing to `uv sync --extra embeddings`. Handles truncation (>1022 residues), empty FASTA, and short sequences.
- `tests/fixtures/query_embeddings.parquet` — Pre-computed embeddings for all 500 Tier 1 query sequences (~600KB). Committed to the repo so downstream tests have real embeddings.
- `tests/test_embedding_features.py` — 13 tests: 6 loader unit tests (always run), 4 fixture validation tests (skip if fixture absent), 3 compute integration tests (skip if torch/esm unavailable — determinism, batch boundary, schema).
- Updated `src/distributed_alignment/features/__init__.py` — exports `load_embeddings`, `EMBEDDING_SCHEMA`, `EMBEDDING_DIM`.
- Updated `pyproject.toml` — added `embeddings = ["fair-esm>=2.0", "torch>=2.0"]` optional dependency group.
- Updated `Makefile` — added `compute-embeddings` target for fixture regeneration.

**Decisions made**:
- `esm2_t6_8M_UR50D` — the smallest ESM-2 variant (8M parameters, 320-dim, 6 layers). Fast enough for CPU on 500 sequences (~4 minutes), small model download (~30MB). Larger variants (35M/150M/650M) would give better representations but the fixture generation time and model size don't justify it for a portfolio project.
- Mean-pooling over residue positions for sequence-level embeddings, rather than CLS token. This is the ESM documentation's recommendation for sequence-level tasks — the CLS token in ESM-2 doesn't have the same trained-for-classification semantics as in BERT. Mean-pooling averages all residue representations, giving a more stable signal.
- Clean separation: the loader module has zero torch dependency. This means importing `distributed_alignment.features` never triggers a torch import. The compute script is a standalone entry point that users opt into by installing the `embeddings` extra. This avoids the common antipattern of optional dependencies that break imports.
- Fixture committed to repo. 500 sequences × 320 floats × 4 bytes = ~640KB compressed — small enough to commit. This means all loader tests and Task 3.5 tests work without torch installed.
- Sequences >1022 residues are truncated with a warning log. ESM-2's context window is 1024 tokens (including BOS/EOS = 1022 residues). 16 of the 500 Tier 1 sequences hit this limit. Truncation loses C-terminal information but is the standard approach — the alternative (sliding window + pooling) adds complexity for marginal gain.

**Problems encountered**:
- `pytest.importorskip("torch")` at module level skips the **entire** test file, not just the class below it. Restructured to use a try/except at module level setting `_has_esm = True/False`, then `@pytest.mark.skipif(not _has_esm, ...)` on the integration test class.
- Torch emits a `UserWarning: Failed to initialize NumPy` on import because numpy isn't in the default environment. Harmless — ESM-2 doesn't need numpy for inference. Shows as a pytest warning but doesn't affect results.
- Typer CLI arguments using `typer.Option(...)` as default values triggers ruff B008 ("Do not perform function call in argument defaults"). Fixed by using the `Annotated[type, typer.Option(...)]` pattern, matching the main project CLI.

**Learnings**:
- ESM-2 in eval mode on CPU is deterministic without needing an explicit `torch.manual_seed()`. Two runs on identical input produce bitwise-identical float32 embeddings. This is because the model has no dropout or stochastic layers active in eval mode.
- The fixture took ~4 minutes for 500 sequences on CPU. Batch processing time varies significantly by sequence length — batches with long sequences (>500 residues) take 20+ seconds, while short-sequence batches take 4-5 seconds.
- `pa.FixedSizeListArray.from_arrays()` takes a flat array — same pattern as k-mer features. For N sequences × 320 dims, pass a flat array of N×320 floats with `list_size=320`.
- Briefly implemented multi-scale pooling (mean + max + std, 960-dim) but reverted to mean-only (320-dim) for consistency with ESM-2 documentation. Mean-pooling is the canonical choice for sequence-level tasks with ESM-2; a custom pooling scheme is harder to defend than "I followed the model authors' recommendation." If richer representations are needed, the principled approach is to use a larger ESM-2 variant (e.g. `esm2_t12_35M_UR50D`, 480-dim) rather than inventing a pooling strategy.
- Makefile `compute-embeddings` target accepts `EMBED_FASTA`, `EMBED_OUTPUT`, `EMBED_RUN_ID` variables with Tier 1 defaults, so it's usable for arbitrary FASTA inputs.

**Status**: Complete

---

### Task 3.5: Feature combiner and schema versioning — 2026-04-18

**What was done**: Built the feature combiner that joins alignment features, k-mer frequencies, and optional ESM-2 embeddings into a single versioned feature table.

Files created:
- `src/distributed_alignment/features/combiner.py` — `combine_features()` joins alignment + k-mer (inner join, validated ID agreement) and optionally left-joins ESM-2 embeddings. Two module-level schemas: `COMBINED_SCHEMA` (with embeddings) and `COMBINED_SCHEMA_NO_EMBEDDINGS` (without). When embeddings aren't provided, the `esm2_embedding` column is absent from the output — distinguishable by schema alone. Per-stream metadata columns (feature_version, run_id, created_at) are dropped from inputs; the combined table has its own.
- `tests/test_combiner.py` — 16 tests across 5 classes: core joins (no embeddings, with embeddings, row count, zero-hit preservation), schema/metadata (both schemas, version/run_id propagation, UTC timestamp, no duplicate metadata), edge cases (partial embeddings, extra sequences dropped, ID mismatch ValueError, empty inputs, missing file), determinism.
- Updated `src/distributed_alignment/features/__init__.py` — exports `combine_features`, `COMBINED_SCHEMA`, `COMBINED_SCHEMA_NO_EMBEDDINGS`.

**Decisions made**:
- Inner join for alignment + k-mer (they must agree — same chunks produced both). A mismatch is surfaced as a `ValueError` with counts and up to 5 example mismatched IDs, rather than silently dropping rows. Left join for embeddings (optional — sequences without embeddings get null, extra embeddings are dropped).
- When embeddings are not provided, the `esm2_embedding` column is absent from the output (not present with all nulls). This keeps the two schemas cleanly distinguishable and avoids downstream code needing to check for an all-null column.
- Variable-size `pa.list_(pa.float32())` for the embedding column in the combined table instead of fixed-size `pa.list_(pa.float32(), 320)`. Fixed-size lists with null entries don't survive Parquet round-trips in the current PyArrow version — nulls are written as size-0 entries instead of proper Arrow nulls, causing `ArrowInvalid` on read. The dimensionality is already validated at `load_embeddings()` time; the combined table just needs nullable list storage.
- DuckDB for the alignment+kmer join (consistent with the rest of the codebase). The embedding left-join is done in Python/PyArrow because DuckDB has the same fixed-size-list null issue.

**Problems encountered**:
- PyArrow/Parquet fixed-size list null round-trip bug: `pa.list_(pa.float32(), 320)` columns with null entries are written to Parquet with size-0 entries instead of proper nulls. Reading them back fails with `ArrowInvalid: Expected all lists to be of size=320 but index N had size=0`. Tried three approaches (DuckDB LEFT JOIN, `pa.array()` with None, `pa.Array.from_buffers()` with validity bitmap) — all produced the same corrupted Parquet. Fixed by using variable-size `pa.list_(pa.float32())` which handles nulls correctly.
- The `FeatureRow` Pydantic model in `models.py` exists but is stale — it has alignment features + metadata but no k-mer or embedding fields. Left it as-is since representing 8,000-dim and 320-dim list fields in Pydantic is awkward, and PyArrow schema validation is doing the real work.

**Learnings**:
- Parquet's handling of nested types (lists, fixed-size lists, structs) with nulls is a known pain point. The Parquet format doesn't have a native fixed-size list type — Arrow maps it using metadata. When nulls are involved, the write/read roundtrip can break. The pragmatic workaround: use variable-size lists in storage, validate dimensions in application code.
- ID agreement validation before a join is cheap and catches data quality bugs that would otherwise silently drop rows. The cost of the set comparison is negligible compared to the IO of reading the Parquet files.

**Status**: Complete

---

### Task 3.6: Data catalogue — 2026-04-18

**What was done**: Built the metadata store that will track pipeline outputs when 3.7 wires everything together. A DuckDB-backed catalogue with three tables (datasets, lineage, runs) and a recursive CTE for ancestry/descendant queries.

Files created:
- `src/distributed_alignment/catalogue/store.py` — `CatalogueStore` class that mirrors the `TaxonomyDB` pattern: owns a DuckDB connection, encapsulates all SQL, exposes domain-level methods. Three tables with the schemas from the TDD plus `git_commit` on runs. Recursive CTE for lineage walks in both directions. JSON columns for `parameters`, `config`, `metrics` with dict round-trip. Context manager support.
- `src/distributed_alignment/catalogue/__init__.py` — exports `CatalogueStore`.
- `tests/test_catalogue.py` — 27 tests across 5 classes: initialisation (tables created, data persists across reopen, context manager, idempotent close, parent dirs), datasets (register+get, minimal/full fields, upsert, filters, null `created_by_run` for reference data), runs (status transitions, `complete_run`/`fail_run`, JSON round-trip, `git_commit` optional, filter by status), lineage (simple edges, chains, branching, descendants, `max_depth` cap, upsert), and FK-ish behaviour (edges referencing unknown datasets are allowed).

**Decisions made**:
- Upsert semantics for all three `register_*` methods, using DuckDB's `INSERT ... ON CONFLICT ... DO UPDATE`. Re-running a pipeline stage re-registers its outputs; raising on conflict would force callers to handle "does this already exist?" logic that belongs in the store. `register_run` specifically only updates `started_at`/`config`/`git_commit` on conflict — `completed_at`/`status`/`metrics` are managed by `complete_run`/`fail_run`.
- Lineage edges may reference unregistered datasets. Pipeline stages often register edges before both datasets are committed; enforcing referential integrity would make the calling code rigid. The catalogue is a thin recorder; 3.7 handles ordering via its own conventions. An explicit test documents this as intentional rather than a bug.
- Added `git_commit` as a column on runs even though it's not in the TDD. One-line bridge between "which pipeline run produced this?" (lineage) and "which code produced this?" (git). Caller provides it; the catalogue doesn't auto-detect (that would couple it to a git working tree).
- Stored timestamps as naive UTC `TIMESTAMP` rather than `TIMESTAMP WITH TIME ZONE`. DuckDB's aware-timestamp binding requires `pytz` which isn't a project dependency. Convert to naive UTC on insert, document the convention. Consistent with `alignment_features.py`'s schema.
- Used `json.dumps(..., default=str)` for JSON columns. Non-JSON-serialisable values (datetimes, Paths) survive as their string form. The catalogue is for inspection, not precise round-tripping — this tradeoff favours robustness.

**Problems encountered**:
- DuckDB's `TIMESTAMP WITH TIME ZONE` column type requires `pytz` at read time: `InvalidInputException: Required module 'pytz' failed to import`. Initial implementation used tz-aware timestamps and failed 14 of 27 tests. Switched to naive `TIMESTAMP` with a `_to_naive_utc()` helper on insert.
- The `B017` ruff rule flagged `with pytest.raises(Exception):` in the context-manager test. Switched to `pytest.raises((AttributeError, AssertionError))` — more specific, and actually correct given the `assert self._conn is not None` guards in the store methods.

**Learnings**:
- DuckDB recursive CTEs work cleanly for lineage. The `WITH RECURSIVE walk(...) AS (anchor UNION ALL recursive-step) SELECT...` pattern with a `depth < max_depth` guard is concise and efficient. The same template covers both ancestors (join on `child = walk.dataset`) and descendants (join on `parent = walk.dataset`) — only the column names swap.
- The "single-writer, multi-reader" model for DuckDB is the right shape for a pipeline metadata store. The catalogue is written by the orchestrator and read by everyone else; trying to make it thread-safe at the object level would add complexity without value.
- Deferred FK enforcement is a judgement call with a real tradeoff: you get flexibility at registration time but lose catch-at-write integrity. The right answer depends on who owns ordering. For this project, the pipeline driver (3.7) owns ordering, so the catalogue can be permissive. A future `validate()` method could flag orphan edges without changing the write path.

**Status**: Complete

---

### Task 3.7: Pipeline integration and CLI wiring — 2026-04-18

**What was done**: Chained the Phase 3 components into a single orchestrated pipeline, surfaced it through new flags on the existing `run` CLI command, and added catalogue registration end-to-end. Also closed the timestamp inconsistency flagged in 3.6.

Files created/modified:
- `src/distributed_alignment/pipeline.py` (new) — `run_feature_pipeline()` pure function, `PipelineResult` dataclass, `RunCollisionError`. Orchestrates merge → enrich (optional) → alignment + k-mer features → embeddings load (optional) → combine. Registers every output and lineage edge in the catalogue with the dataset-then-edge ordering convention. Graceful degradation across the full coverage matrix.
- `src/distributed_alignment/cli.py` (modified) — added 8 new Phase 3 flags to the `run` command (`--taxonomy-db`, `--embeddings`, `--skip-enrichment`, `--skip-features`, `--feature-version`, `--run-id`, `--git-commit`, `--force-rerun`). The CLI becomes a thin wrapper calling `run_feature_pipeline()`.
- `src/distributed_alignment/config.py` — added `taxonomy_db_path` and `embeddings_path` fields.
- `src/distributed_alignment/features/alignment_features.py` — detects missing `phylum`/`kingdom` columns and injects NULL placeholders so the function handles both enriched and non-enriched inputs. Also migrated `created_at` to tz-aware UTC.
- `src/distributed_alignment/catalogue/store.py` — switched to `TIMESTAMP WITH TIME ZONE`, removed the `_to_naive_utc()` helper, documented the tz-aware convention in the module docstring.
- `pyproject.toml` — added `pytz>=2024.1` to core deps.
- `tests/test_integration.py` — 13 new Phase 3 integration tests across 5 classes (coverage matrix, catalogue integration, rerun semantics, failure path, CLI smoke). These run in the default `make test` suite since they don't need DIAMOND.

**Decisions made**:
- **Timestamp option 1** (add pytz, all-tz-aware). There was a three-way inconsistency — alignment features naive, kmer/embedding tz-aware, catalogue naive. Keeping the catalogue naive would leave Parquet ↔ catalogue columns diverged forever. Adding `pytz` as a core dep is a small cost for a coherent codebase; DuckDB's Python binding specifically wants `pytz` (not `zoneinfo`) at bind time. Every `created_at`, `started_at`, `completed_at` in the project is now `pa.timestamp("us", tz="UTC")` or DuckDB `TIMESTAMP WITH TIME ZONE`.
- **Extracted to `pipeline.py`** rather than inlining in `cli.py`, contrary to the prompt's "err toward inline" steer. Reasoning: the orchestration is ~180 lines with conditional stage branching, per-stage catalogue registration, and error handling. More importantly, the integration tests are much cleaner when they can call the pipeline function directly with typed kwargs rather than driving it through Typer's `CliRunner`. `cli.py` becomes a ~20-line wrapper that handles flag plumbing. The prompt explicitly invited this decision to be justified.
- **`--force-rerun` safety check**: added as specified. A run_id collision (existing run that's not "failed") raises `RunCollisionError` unless `--force-rerun` is passed. This catches accidental overwrites — the upsert semantics of `register_run` alone would silently re-register a completed run as "running", which masks actual completion time.
- **Dataset-then-edge ordering**: for each stage, the pipeline registers the output dataset first, then the lineage edge(s) from parents → child. This matches the 3.6 review's recommendation — if a crash happens mid-registration, you end up with an orphan dataset (recoverable) rather than an orphan edge (confusing). The catalogue is permissive about FK integrity, so this discipline lives in the pipeline driver.
- **Embeddings as reference dataset**: when `--embeddings` is provided, the embeddings file is registered in the catalogue with `created_by_run=None`. Pre-computed externally, so no producing run to link to. Re-registering the same embeddings across runs is a no-op via upsert.
- **Non-enriched alignment features**: rather than modify `alignment_features.py`'s SQL to branch on column existence, I inject NULL `phylum`/`kingdom` columns at the boundary when the input lacks them. Minimal change, no new code paths in the SQL, tests that already covered enriched input still pass as-is.

**Problems encountered**:
- Initial integration tests hit `_duckdb.BinderException: Referenced column "phylum" not found` when running without `--taxonomy-db`. Fix: the NULL-column injection described above.
- Typer's `Annotated[type, typer.Option(...)]` pattern is necessary for boolean flags to work correctly — using raw `typer.Option()` as a default triggers ruff B008 and produces surprising behaviour for flag defaults.
- The pipeline initially had `run_id: str` as a positional arg in `cli.py`, colliding with `q_manifest.run_id` from the existing merge flow. Fixed with `effective_run_id = run_id or q_manifest.run_id`.

**Learnings**:
- Keeping the orchestrator as a pure function with no Typer dependencies was worth the effort. The integration tests read like normal Python and the CLI stays thin enough that its only job is flag translation.
- The dataset-then-edge registration order matters more than it looks. Without it, a failure mid-registration leaves dangling lineage rows that point to nothing — confusing when debugging. With it, the worst case is a dataset row with no downstream lineage, which is obvious.
- `PipelineResult` as a dataclass return type gives callers typed access to output paths and metrics without re-reading the catalogue. The CLI uses it for the summary echo; the tests use it for assertions.

**Status**: Complete