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

### Task 1.1: Streaming FASTA parser

**What**: Parse FASTA files as a generator, yielding validated ProteinSequence objects.

**Files**:
- `src/distributed_alignment/ingest/__init__.py`
- `src/distributed_alignment/ingest/fasta_parser.py`
- `tests/test_fasta_parser.py`
- `tests/fixtures/` — small test FASTA files (valid, empty, malformed)

**Key behaviours**:
- Generator-based: yields one ProteinSequence at a time, never loads entire file
- Validates amino acid alphabet (standard 20 + common ambiguity codes B, J, O, U, X, Z)
- Handles multi-line sequences (FASTA sequences can wrap across lines)
- Raises clear errors on: empty sequences, invalid characters, malformed headers
- Logs a warning (doesn't crash) on sequences exceeding a configurable max length

**Tests**:
- Parse a valid multi-sequence FASTA → correct IDs, sequences, lengths
- Parse a FASTA with multi-line sequences → correctly concatenated
- Empty file → yields nothing (no error)
- Malformed header (no '>') → raises ValueError with line number
- Invalid amino acid characters → raises ValueError identifying the bad characters
- Generator behaviour: parsing a 1000-sequence file uses O(1) memory (test via tracemalloc or just assert it yields)

---

### Task 1.2: Deterministic chunker

**What**: Split a stream of ProteinSequence objects into deterministic, hash-based chunks, writing each chunk as Parquet.

**Files**:
- `src/distributed_alignment/ingest/chunker.py`
- `tests/test_chunker.py`

**Key behaviours**:
- Assigns each sequence to a chunk via `hash(sequence_id) % num_chunks`
- Uses a stable hash (hashlib SHA-256, not Python's built-in hash which is randomised)
- Writes each chunk as a Parquet file with the schema defined in the TDD
- Produces a ChunkManifest (JSON) with chunk metadata and content checksums
- Streaming: accumulates sequences per chunk in memory, flushes to Parquet when all input is consumed (for Phase 1 this is fine; streaming flush can be added later)

**Tests**:
- Determinism: chunk the same FASTA twice → identical Parquet files (byte-for-byte via checksum)
- Determinism across ordering: shuffle input order → same chunk assignments
- Round-trip: chunk → read all Parquet chunks → reassemble → compare against original (zero loss, zero duplication)
- Chunk count: 100 sequences into 5 chunks → each chunk has ~20 sequences (within reasonable variance)
- Manifest accuracy: checksums match actual file checksums, sequence counts are correct
- Edge case: fewer sequences than chunks → some chunks are empty (valid, just no Parquet file)

---

### Task 1.3: Work package scheduler

**What**: Generate work packages from the Cartesian product of query and reference chunk manifests. Implement the filesystem-backed work stack with atomic claims.

**Files**:
- `src/distributed_alignment/scheduler/__init__.py`
- `src/distributed_alignment/scheduler/protocols.py` — WorkStack protocol
- `src/distributed_alignment/scheduler/filesystem_backend.py`
- `tests/test_work_stack.py`

**Key behaviours**:
- `generate_work_packages(query_manifest, ref_manifest)` → creates Q×R work package JSON files in `pending/` directory
- `claim(worker_id)` → atomically moves one package from `pending/` to `running/`, returns WorkPackage or None
- `complete(package_id, result_path)` → moves from `running/` to `completed/`
- `fail(package_id, error)` → increments attempt, moves to `pending/` if retries remain, else to `poisoned/`
- `status()` → returns counts by state
- Atomic claim via `os.rename()` — if two workers race, exactly one succeeds

**Tests**:
- Generate packages: 3 query chunks × 2 ref chunks → 6 packages, all PENDING
- Claim: claim from 6 pending → returns a package, pending count decrements
- Claim from empty → returns None
- Complete: package moves to completed, status reflects it
- Fail with retries remaining → back to pending with incremented attempt
- Fail with max attempts exhausted → moves to poisoned
- Concurrent claims (use threading): 10 threads claiming from 5 packages → each package claimed exactly once, no errors
- Status accuracy: counts match actual directory contents

---

### Task 1.4: DIAMOND wrapper and worker

**What**: Execute DIAMOND blastp as a subprocess for a single work package. Parse output into Arrow/Parquet.

**Files**:
- `src/distributed_alignment/worker/__init__.py`
- `src/distributed_alignment/worker/diamond_wrapper.py`
- `src/distributed_alignment/worker/runner.py`
- `tests/test_diamond_wrapper.py`
- `tests/test_worker.py`
- `scripts/download_test_data.sh` — fetch a small reference DB for testing

**Key behaviours**:
- `DiamondWrapper.run(query_fasta, ref_db, output_path, **kwargs)` → runs DIAMOND, returns exit code + timing
- Handles DIAMOND's `makedb` step (building the .dmnd database from FASTA)
- Parses DIAMOND tabular output (format 6) into a PyArrow Table
- `WorkerRunner.run()` → main loop: claim → fetch → align → parse → write Parquet → complete → repeat
- Exit code handling: 0=success, 137=OOM, other=general failure
- Subprocess timeout (configurable)

**Test dataset**:
- ~100 query sequences, ~1000 reference sequences (small enough to align in seconds)
- Can use a subset of Swiss-Prot or generate synthetic sequences

**Tests**:
- Diamond wrapper: run on test data → produces non-empty output file
- Parse output: tabular format 6 → PyArrow Table with correct column names and types
- Worker loop: with 3 pending packages, worker processes all 3 and they end up in completed
- Timeout: set a very short timeout on a large-ish alignment → subprocess.TimeoutExpired handled gracefully
- Missing DIAMOND binary: clear error message (not a cryptic FileNotFoundError)

**Note**: Tests that actually run DIAMOND require the binary to be installed. Mark these with `@pytest.mark.integration` so they can be skipped in CI initially.

---

### Task 1.5: Result merger

**What**: Merge alignment results across reference chunks for each query chunk, dedup and rank.

**Files**:
- `src/distributed_alignment/merge/__init__.py`
- `src/distributed_alignment/merge/merger.py`
- `tests/test_merger.py`

**Key behaviours**:
- `merge_query_chunk(query_chunk_id, results_dir, output_dir, top_n)` → reads all result Parquet files for the query chunk, deduplicates, ranks globally, writes merged Parquet
- Uses DuckDB for the merge (SQL with ROW_NUMBER window function)
- Validates output schema before writing
- Detects incomplete merges (not all reference chunks have results yet) and raises rather than producing partial output

**Tests**:
- Merge 3 result files for one query chunk → correct global ranking by evalue
- Deduplication: same query-subject pair appears in multiple ref chunks → kept once (best score)
- Top-N: with top_n=5, no query has more than 5 hits in output
- Schema validation: output has all expected columns with correct types
- Incomplete merge: 2 of 3 expected result files present → raises with clear message
- Empty results: a query chunk with no hits across any ref chunk → valid empty Parquet (correct schema, zero rows)

---
