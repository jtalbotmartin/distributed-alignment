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

### Task 1.6: Structured logging setup

**What**: Configure structlog for JSON logging with correlation IDs.

**Files**:
- `src/distributed_alignment/observability/__init__.py`
- `src/distributed_alignment/observability/logging.py`
- Update existing modules to use structured logging

**Key behaviours**:
- `configure_logging(level, run_id)` → sets up structlog with JSON output, bound with run_id
- Every module uses `structlog.get_logger()` and binds component-specific context (worker_id, package_id, etc.)
- Log levels: DEBUG for detailed tracing, INFO for pipeline events, WARNING for recoverable issues, ERROR for failures

**Tests**:
- Logging configuration produces JSON output
- Bound context (run_id, package_id) appears in log entries
- Different components produce logs with correct component field

---

### Task 1.7: End-to-end integration and CLI wiring

**What**: Wire everything together so the CLI runs the full pipeline. Write the integration test.

**Files**:
- Update `src/distributed_alignment/cli.py` — implement `ingest` and `run` subcommands
- `tests/test_integration.py`
- Update `README.md` with quickstart

**Key behaviours**:
- `distributed-alignment ingest --queries <path> --reference <path> --output-dir <path>` → parses, validates, chunks both query and reference, writes manifests
- `distributed-alignment run --work-dir <path> --workers 1` → generates work packages, runs single worker, merges results
- `distributed-alignment status --work-dir <path>` → prints run summary

**Integration test**:
- Uses the test dataset from Task 1.4
- Runs the full pipeline: ingest → schedule → align → merge
- Verifies: all packages completed, merged Parquet exists with correct schema, results are queryable via DuckDB
- Single test function, marked `@pytest.mark.integration`

**Acceptance criteria for Phase 1 complete**:
- `distributed-alignment ingest` + `distributed-alignment run` produces queryable Parquet results
- `pytest tests/ -v` all green (unit + integration with DIAMOND installed)
- `pytest tests/ -v -m "not integration"` all green (unit only, no DIAMOND needed)
- `ruff check src/ tests/` clean
- `mypy src/ --strict` clean

---

# `distributed-alignment` — Phase 2 Task Breakdown

## Phase 2: Fault Tolerance & Distribution

**Goal**: Multi-worker execution with fault recovery, containerised deployment, metrics, monitoring, and CI/CD.

This is the phase that makes the project impressive. Phase 1 proved the pipeline works end-to-end; Phase 2 proves it works when things go wrong and when multiple workers are competing for work.

---

### What Phase 2 covers (from TDD §9)

- Heartbeat mechanism and timeout reaper
- Retry with backoff, poison queue for permanent failures
- Ray integration (workers as Ray actors)
- Docker packaging (Dockerfile + docker-compose with N workers)
- Chaos tests: kill worker mid-package, verify recovery
- Prometheus metrics exposition
- Grafana dashboard JSON
- GitHub Actions CI/CD pipeline

### Breakdown

Phase 2 breakdown — 10 tasks (2.0 through 2.9), ordered so each builds on the last.

The key structural decision is multiprocessing before Ray (Task 2.3 before 2.5). This matters because the hard problems in Phase 2 — atomic claims under real concurrency, heartbeat/reaper race conditions, worker death recovery — are all concurrency problems, not Ray problems. Getting them right with multiprocessing (which is simpler to debug, no cluster setup, no Ray overhead) means that when you add Ray, you're just swapping the execution backend, not debugging distributed systems issues and Ray issues simultaneously.

The other thing worth noting is the ordering of 2.1 → 2.2 → 2.3 → 2.4. Heartbeats are useless without a reaper, the reaper is meaningless without multiple workers, and chaos tests validate the whole chain. Each task has a clear "this is why this exists" that references the previous one.

Tasks 2.6-2.9 (metrics, Grafana, Docker, CI/CD) are more independent and could be done in any order. I put them after the core concurrency work because they're less architecturally risky — they're important for the portfolio but unlikely to surface bugs in the core system.

---

## Task Breakdown

### Task 2.0: Wire config into CLI + minor Phase 1 cleanup

**What**: The `DistributedAlignmentConfig` class exists but the CLI hard-codes values as Typer defaults. Wire the config so that `distributed_alignment.toml` settings are respected, with CLI flags as overrides.

**Why now**: Phase 2 adds config for heartbeat intervals, timeouts, worker counts, and Ray settings. The config needs to actually work before we add more fields to it.

**Key behaviours**:
- CLI loads config from `distributed_alignment.toml` if present in the working directory, then applies CLI flag overrides
- Environment variables (`DA_*`) override the TOML file
- CLI flags override everything
- Add new config fields needed for Phase 2: `heartbeat_interval`, `heartbeat_timeout`, `max_attempts`, `num_workers`

**Files**:
- Update `src/distributed_alignment/config.py`
- Update `src/distributed_alignment/cli.py`
- Update `tests/test_scaffolding.py` or create `tests/test_config.py`

---

### Task 2.1: Heartbeat mechanism

**What**: Workers send periodic heartbeats while processing a work package. The work stack tracks the last heartbeat timestamp per running package.

**Key behaviours**:
- `WorkerRunner` starts a background thread that calls `work_stack.heartbeat(package_id)` every N seconds while a package is being processed
- `heartbeat()` updates the `heartbeat_at` timestamp in the work package JSON
- The heartbeat thread starts when a package is claimed and stops when it's completed or failed
- Thread-safe: the heartbeat thread and the main worker thread both access the work package file

**Files**:
- Update `src/distributed_alignment/worker/runner.py`
- Update `src/distributed_alignment/scheduler/filesystem_backend.py` (heartbeat method may need implementation)
- `tests/test_heartbeat.py`

**Tests**:
- Worker processing a package updates heartbeat_at periodically
- heartbeat_at is more recent than claimed_at after processing starts
- Heartbeat thread stops cleanly when package completes
- Heartbeat thread stops cleanly when package fails

---

### Task 2.2: Timeout reaper

**What**: A reaper process that detects stale heartbeats and re-enqueues timed-out packages.

**Key behaviours**:
- `reap_stale(timeout_seconds)` on the work stack scans `running/` for packages where `now - heartbeat_at > timeout`
- Timed-out packages are moved back to `pending/` with attempt incremented and `TIMEOUT` recorded in error_history
- If a package has exhausted max_attempts via timeouts, it goes to `poisoned/`
- The reaper can run as a background thread in any worker, or as a standalone process
- For Phase 2: run the reaper as a background thread in the WorkerRunner, checking every `reaper_interval` seconds

**Files**:
- Update `src/distributed_alignment/scheduler/filesystem_backend.py` (implement/refine `reap_stale`)
- Update `src/distributed_alignment/worker/runner.py` (reaper background thread)
- `tests/test_reaper.py`

**Tests**:
- Package with stale heartbeat (older than timeout) gets moved back to PENDING
- Package with fresh heartbeat is left alone
- Package with max attempts exhausted goes to POISONED
- Reaper handles empty running directory gracefully
- Reaper doesn't interfere with actively running packages (heartbeat is recent)
- Concurrent: reaper runs while worker is processing — no race conditions

---

### Task 2.3: Multi-worker execution (multiprocessing, no Ray yet)

**What**: Run N workers concurrently using Python multiprocessing. This is the stepping stone to Ray — get the concurrency semantics right with simpler tooling first.

**Why multiprocessing before Ray**: The atomic claim mechanism, heartbeats, and reaper all need to work correctly with real concurrency before adding Ray's complexity. Multiprocessing gives us real parallel execution with real race conditions to test against.

**Key behaviours**:
- `distributed-alignment run --work-dir work/ --workers 4` spawns 4 worker processes
- Each worker runs an independent WorkerRunner loop
- Workers compete for packages via the atomic claim mechanism
- The reaper runs as a background thread in one (or all) workers
- When all packages are processed, workers exit cleanly
- The CLI waits for all workers to finish before running the merge step

**Files**:
- Update `src/distributed_alignment/cli.py` (multi-worker support)
- Update `src/distributed_alignment/worker/runner.py` (if needed for multi-process compatibility)
- `tests/test_multiworker.py`

**Tests**:
- 4 workers processing 8 packages → all 8 completed, none duplicated
- Worker death (kill one process mid-run) → its package is eventually reclaimed via reaper and completed by another worker
- All workers exit cleanly when work is exhausted
- Status shows correct counts throughout

---

### Task 2.4: Chaos testing

**What**: Explicit tests that simulate failures and verify recovery. These are the tests that prove the system is fault-tolerant, not just concurrent.

**Key behaviours**:
- Kill a worker process mid-alignment (SIGKILL) → package times out → reaper reclaims → another worker completes it
- Kill a worker between claim and execution start → same recovery path
- Simulate OOM (mock DIAMOND returning exit code 137) → package fails → retried → eventually succeeds or poisons
- Corrupt a work package JSON file → worker handles gracefully, doesn't crash the loop
- Fill up the results directory (mock disk full) → worker fails the package cleanly

**Files**:
- `tests/test_chaos.py`

**Tests**: All marked `@pytest.mark.integration` (they need real multi-process execution and potentially DIAMOND).

---

### Task 2.5: Ray integration

**What**: Replace multiprocessing with Ray for worker management. Ray provides task-level fault tolerance, resource management, and a foundation for elastic scaling on K8s.

**Key behaviours**:
- `AlignmentWorker` as a Ray actor with configurable CPU/memory resources
- `distributed-alignment run --workers N --backend ray` uses Ray; `--backend local` uses multiprocessing (default)
- Ray in local mode for development (no cluster needed); Ray on a cluster for production
- The WorkerRunner logic is unchanged — Ray just manages where and how many instances run
- Ray dashboard available at localhost:8265 when running

**Files**:
- `src/distributed_alignment/worker/ray_actor.py`
- Update `src/distributed_alignment/cli.py` (Ray backend option)
- Update `pyproject.toml` (add ray dependency)
- `tests/test_ray_worker.py`

**Tests**:
- Ray actor processes packages correctly (same results as multiprocessing)
- Multiple Ray actors process packages concurrently without conflicts
- Ray actor failure → Ray restarts it → package is reclaimed
- Mark Ray tests as `@pytest.mark.integration` (Ray adds significant startup time)

---

### Task 2.6: Prometheus metrics

**What**: Expose pipeline metrics in Prometheus format for monitoring.

**Key behaviours**:
- Metrics from TDD §3.8: `da_packages_total` (gauge by state), `da_package_duration_seconds` (histogram), `da_sequences_processed_total` (counter), `da_hits_found_total` (counter), `da_worker_heartbeat_age_seconds` (gauge), `da_worker_count` (gauge), `da_errors_total` (counter by type), `da_diamond_exit_code` (counter by code)
- Metrics exposed via an HTTP endpoint (prometheus_client start_http_server) on a configurable port
- Metrics updated by the worker during processing
- Metrics server runs as a background thread, doesn't block the pipeline

**Files**:
- `src/distributed_alignment/observability/metrics.py`
- Update `src/distributed_alignment/worker/runner.py` (emit metrics)
- Update `pyproject.toml` (add prometheus_client dependency)
- `tests/test_metrics.py`

**Tests**:
- Metrics server starts and exposes /metrics endpoint
- Processing a package updates the relevant counters/histograms
- Package state transitions update da_packages_total gauge
- Metrics are correct after processing N packages

---

### Task 2.7: Grafana dashboard

**What**: Pre-configured Grafana dashboard JSON that visualises the Prometheus metrics.

**Key behaviours**:
- Dashboard with panels from TDD §3.8: pipeline progress, worker health, resource usage, cost estimate, error analysis
- Auto-provisioned via Grafana's provisioning mechanism in docker-compose
- Connects to Prometheus as a data source

**Files**:
- `observability/grafana/dashboards/distributed-alignment.json`
- `observability/grafana/provisioning/dashboards.yml`
- `observability/grafana/provisioning/datasources.yml`
- `observability/prometheus.yml` (scrape config targeting the metrics endpoint)
- Update `docker-compose.yml` (add Prometheus and Grafana services)

**Tests**: Manual — verify dashboard loads and shows data when pipeline runs via docker-compose.

---

### Task 2.8: Docker packaging (production-ready)

**What**: Upgrade from the dev Dockerfile to a proper multi-stage build. Update docker-compose with multi-worker + observability stack.

**Key behaviours**:
- Multi-stage Dockerfile: build stage (uv sync, compile) → runtime stage (slim, just DIAMOND + Python + installed packages)
- docker-compose.yml with services: N workers (configurable replica count), Prometheus, Grafana, optional explorer (stub for Phase 4)
- Shared volume for the work directory
- Health checks on worker containers

**Files**:
- `Dockerfile` (production, replaces or supplements Dockerfile.dev)
- Update `docker-compose.yml`
- `docker-compose.override.yml` (dev overrides: source mounting, debug ports)

---

### Task 2.9: GitHub Actions CI/CD

**What**: Automated quality checks on every push and PR.

**Key behaviours**:
- Lint job: ruff check + ruff format --check
- Type check job: mypy --strict
- Unit test job: pytest -m "not integration" with coverage report
- Docker build job: build the image, verify it starts
- Integration test job (optional, runs on main only): docker-compose with DIAMOND, runs integration tests
- Upload coverage to Codecov or similar

**Files**:
- `.github/workflows/ci.yml`
- Possibly `.github/workflows/integration.yml` (separate workflow for heavier tests)

---

## Implementation Order

Tasks are ordered to build on each other:

1. **2.0** — Config wiring (foundation for all Phase 2 config)
2. **2.1** — Heartbeats (needed before reaper)
3. **2.2** — Reaper (needed before multi-worker makes sense)
4. **2.3** — Multi-worker via multiprocessing (validates concurrency)
5. **2.4** — Chaos tests (validates fault tolerance)
6. **2.5** — Ray integration (upgrades the worker backend)
7. **2.6** — Prometheus metrics (observability)
8. **2.7** — Grafana dashboard (visualisation of metrics)
9. **2.8** — Docker packaging (deployment)
10. **2.9** — CI/CD (automation)


## Acceptance Criteria for Phase 2 Complete

- Multi-worker execution: `--workers 4` processes packages in parallel correctly
- Fault tolerance demonstrated: killing a worker doesn't lose data or stall the pipeline
- Ray backend works: `--backend ray` produces identical results to `--backend local`
- Prometheus metrics exposed and accurate
- Grafana dashboard loads and shows meaningful data during a pipeline run
- docker-compose spins up the full stack (workers + Prometheus + Grafana)
- GitHub Actions CI passes on push
- All existing Phase 1 tests still pass

