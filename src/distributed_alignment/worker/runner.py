"""Worker runner — main loop for claiming and processing work packages.

Claims packages from a WorkStack, executes DIAMOND alignment via
DiamondWrapper, writes result Parquet, and marks packages complete.
"""

from __future__ import annotations

import threading
import time
import uuid
from typing import TYPE_CHECKING

import pyarrow.parquet as pq
import structlog

from distributed_alignment.worker.diamond_wrapper import (
    DiamondWrapper,
    parse_output,
)

if TYPE_CHECKING:
    from pathlib import Path
    from types import TracebackType

    from distributed_alignment.models import WorkPackage
    from distributed_alignment.scheduler.filesystem_backend import (
        FileSystemWorkStack,
    )

logger = structlog.get_logger(component="worker")


def parquet_chunk_to_fasta(parquet_path: Path, fasta_path: Path) -> int:
    """Convert a chunk Parquet file back to FASTA format for DIAMOND.

    DIAMOND reads FASTA, not Parquet, so we need this conversion step
    before running alignment.

    Args:
        parquet_path: Path to the chunk Parquet file.
        fasta_path: Path to write the FASTA output.

    Returns:
        Number of sequences written.
    """
    table = pq.read_table(parquet_path)
    count = 0

    with fasta_path.open("w") as f:
        for i in range(table.num_rows):
            desc = table.column("description")[i].as_py()
            seq = table.column("sequence")[i].as_py()
            f.write(f">{desc}\n")
            # Write in 80-char lines (FASTA convention)
            for start in range(0, len(seq), 80):
                f.write(seq[start : start + 80] + "\n")
            count += 1

    return count


class HeartbeatSender:
    """Context manager that sends periodic heartbeats in a background thread.

    Usage::

        with HeartbeatSender(work_stack, "wp_q000_r000", interval=30):
            do_long_running_work()

    The heartbeat thread is a daemon thread that stops cleanly when the
    context exits (success, failure, or exception). Errors in the heartbeat
    call are logged but don't kill the worker.
    """

    def __init__(
        self,
        work_stack: FileSystemWorkStack,
        package_id: str,
        *,
        interval: float = 30.0,
    ) -> None:
        self._work_stack = work_stack
        self._package_id = package_id
        self._interval = interval
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def __enter__(self) -> HeartbeatSender:
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run,
            daemon=True,
            name=f"heartbeat-{self._package_id}",
        )
        self._thread.start()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.stop()

    def stop(self) -> None:
        """Signal the heartbeat thread to stop and wait for it."""
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=self._interval + 1)

    @property
    def is_alive(self) -> bool:
        """Whether the heartbeat thread is still running."""
        return self._thread is not None and self._thread.is_alive()

    def _run(self) -> None:
        """Heartbeat loop — runs in background thread."""
        while not self._stop_event.wait(timeout=self._interval):
            try:
                self._work_stack.heartbeat(self._package_id)
            except Exception:
                logger.debug(
                    "heartbeat_error",
                    package_id=self._package_id,
                    exc_info=True,
                )
                return


class ReaperThread:
    """Context manager that periodically reaps stale work packages.

    Runs ``work_stack.reap_stale(timeout_seconds)`` every ``interval``
    seconds in a background daemon thread. Reclaims packages whose
    workers have died (stale heartbeats) and returns them to the queue.

    Usage::

        with ReaperThread(work_stack, timeout=120, interval=60):
            run_worker_loop()
    """

    def __init__(
        self,
        work_stack: FileSystemWorkStack,
        *,
        timeout_seconds: int = 120,
        interval: float = 60.0,
    ) -> None:
        self._work_stack = work_stack
        self._timeout_seconds = timeout_seconds
        self._interval = interval
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def __enter__(self) -> ReaperThread:
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run,
            daemon=True,
            name="reaper",
        )
        self._thread.start()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.stop()

    def stop(self) -> None:
        """Signal the reaper thread to stop and wait for it."""
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=self._interval + 1)

    @property
    def is_alive(self) -> bool:
        """Whether the reaper thread is still running."""
        return self._thread is not None and self._thread.is_alive()

    def _run(self) -> None:
        """Reaper loop — runs in background thread."""
        while not self._stop_event.wait(timeout=self._interval):
            try:
                reaped = self._work_stack.reap_stale(
                    self._timeout_seconds
                )
                if not reaped:
                    logger.debug("reaper_scan_clean")
            except Exception:
                logger.warning(
                    "reaper_error",
                    exc_info=True,
                )


class WorkerRunner:
    """Main worker loop: claim → align → write → complete → repeat.

    Uses a polling claim loop with exponential backoff. Exits when
    idle for longer than ``max_idle_time`` (no successful claims).

    Args:
        work_stack: WorkStack to claim packages from.
        diamond: DiamondWrapper for executing alignment.
        chunks_dir: Directory containing chunk Parquet files.
        results_dir: Directory to write result Parquet files.
        sensitivity: DIAMOND sensitivity mode.
        max_target_seqs: Maximum target sequences per query.
        timeout: DIAMOND subprocess timeout in seconds.
        heartbeat_interval: Seconds between heartbeats.
        heartbeat_timeout: Seconds before a package is stale.
        reaper_interval: Seconds between reaper scans.
        max_idle_time: Seconds with no successful claim before exit.
    """

    def __init__(
        self,
        work_stack: FileSystemWorkStack,
        diamond: DiamondWrapper,
        chunks_dir: Path,
        results_dir: Path,
        *,
        sensitivity: str = "very-sensitive",
        max_target_seqs: int = 50,
        timeout: int = 3600,
        heartbeat_interval: float = 30.0,
        heartbeat_timeout: int = 120,
        reaper_interval: float = 60.0,
        max_idle_time: float = 30.0,
    ) -> None:
        self._work_stack = work_stack
        self._diamond = diamond
        self._chunks_dir = chunks_dir
        self._results_dir = results_dir
        self._sensitivity = sensitivity
        self._max_target_seqs = max_target_seqs
        self._timeout = timeout
        self._heartbeat_interval = heartbeat_interval
        self._heartbeat_timeout = heartbeat_timeout
        self._reaper_interval = reaper_interval
        self._max_idle_time = max_idle_time
        self._worker_id = f"worker-{uuid.uuid4().hex[:8]}"
        self._shutdown = threading.Event()

        self._results_dir.mkdir(parents=True, exist_ok=True)

        # Cache directory for reference databases — avoids rebuilding
        # the same .dmnd file when multiple query chunks align against
        # the same reference chunk.
        self._ref_db_cache = self._results_dir.parent / "ref_dbs"
        self._ref_db_cache.mkdir(parents=True, exist_ok=True)

    @property
    def worker_id(self) -> str:
        """Return this worker's unique identifier."""
        return self._worker_id

    def request_shutdown(self) -> None:
        """Signal the worker to stop after finishing current work."""
        self._shutdown.set()

    def run(self) -> int:
        """Run the worker loop with polling and backoff.

        Claims packages from the work stack. When no packages are
        available, retries with exponential backoff (0.5s → 1s → 2s →
        5s max). Exits when idle for ``max_idle_time`` seconds or when
        shutdown is requested.

        A background reaper thread runs alongside the poll loop to
        reclaim packages abandoned by dead workers.

        Returns:
            Number of packages successfully processed.
        """
        completed = 0
        backoff = 0.5
        max_backoff = 5.0
        idle_since: float | None = None

        from distributed_alignment.observability.metrics import (
            dec_worker,
            inc_worker,
            update_package_states,
        )

        logger.info(
            "worker_started",
            worker_id=self._worker_id,
        )
        inc_worker()

        try:
            with ReaperThread(
                self._work_stack,
                timeout_seconds=self._heartbeat_timeout,
                interval=self._reaper_interval,
            ):
                while not self._shutdown.is_set():
                    # Update state gauges on each poll cycle
                    update_package_states(self._work_stack.status())

                    package = self._work_stack.claim(self._worker_id)

                    if package is None:
                        now = time.monotonic()
                        if idle_since is None:
                            idle_since = now

                        idle_duration = now - idle_since
                        if idle_duration >= self._max_idle_time:
                            logger.info(
                                "worker_idle_exit",
                                worker_id=self._worker_id,
                                idle_seconds=round(
                                    idle_duration, 1
                                ),
                            )
                            break

                        time.sleep(backoff)
                        backoff = min(backoff * 2, max_backoff)
                        continue

                    idle_since = None
                    backoff = 0.5

                    with HeartbeatSender(
                        self._work_stack,
                        package.package_id,
                        interval=self._heartbeat_interval,
                    ):
                        success = self._process_package(package)
                    if success:
                        completed += 1
        finally:
            dec_worker()

        logger.info(
            "worker_finished",
            worker_id=self._worker_id,
            completed=completed,
        )

        return completed

    def _process_package(self, package: WorkPackage) -> bool:
        """Process a single work package.

        Args:
            package: The claimed work package.

        Returns:
            True if the package was successfully completed.
        """
        from distributed_alignment.observability.metrics import (
            record_package_completed,
            record_package_failed,
        )

        log = logger.bind(
            package_id=package.package_id,
            worker_id=self._worker_id,
            query_chunk=package.query_chunk_id,
            ref_chunk=package.ref_chunk_id,
        )
        log.info("processing_package")
        start = time.monotonic()

        try:
            result_path, num_hits = self._run_alignment(package)
        except Exception as exc:
            error_msg = f"{type(exc).__name__}: {exc}"
            log.error("package_failed", error=error_msg)
            self._work_stack.fail(package.package_id, error_msg)
            record_package_failed("exception")
            return False

        if result_path is None:
            return False

        duration = time.monotonic() - start
        self._work_stack.complete(
            package.package_id, str(result_path)
        )
        record_package_completed(
            duration_seconds=duration,
            num_sequences=0,  # Not tracked at this level
            num_hits=num_hits,
        )
        log.info(
            "package_completed",
            result_path=str(result_path),
            duration=round(duration, 2),
            hits=num_hits,
        )
        return True

    def _run_alignment(
        self, package: WorkPackage
    ) -> tuple[Path | None, int]:
        """Execute the alignment for a work package.

        Converts Parquet chunks to FASTA, builds the reference DB if
        not already cached, runs DIAMOND blastp, and writes results as
        Parquet.

        Args:
            package: The work package to process.

        Returns:
            Tuple of (result Parquet path, hit count). Path is None
            on failure.
        """
        from distributed_alignment.observability.metrics import (
            record_diamond_result,
            record_package_failed,
        )

        # Locate chunk Parquet files
        query_parquet = self._find_chunk_parquet(
            package.query_chunk_id
        )
        ref_parquet = self._find_chunk_parquet(package.ref_chunk_id)

        if query_parquet is None or ref_parquet is None:
            error = (
                f"Missing chunk file: "
                f"query={query_parquet}, ref={ref_parquet}"
            )
            self._work_stack.fail(package.package_id, error)
            record_package_failed("missing_chunk")
            return None, 0

        # Create temp working directory for this package
        work_dir = self._results_dir / f".tmp_{package.package_id}"
        work_dir.mkdir(parents=True, exist_ok=True)

        try:
            # Convert query Parquet → FASTA
            query_fasta = work_dir / "query.fasta"
            parquet_chunk_to_fasta(query_parquet, query_fasta)

            # Build or reuse cached reference DB
            ref_db_path = self._get_or_build_ref_db(
                package.ref_chunk_id, ref_parquet, work_dir
            )
            if ref_db_path is None:
                error = (
                    "Failed to build reference DB "
                    f"for {package.ref_chunk_id}"
                )
                self._work_stack.fail(package.package_id, error)
                record_package_failed("makedb_failed")
                return None, 0

            # Run alignment
            raw_output = work_dir / "output.tsv"
            blast_result = self._diamond.run_blastp(
                query_fasta,
                ref_db_path,
                raw_output,
                sensitivity=self._sensitivity,
                max_target_seqs=self._max_target_seqs,
                timeout=self._timeout,
            )

            record_diamond_result(blast_result.exit_code)

            if blast_result.exit_code != 0:
                error = (
                    blast_result.error_message
                    or f"blastp failed: exit {blast_result.exit_code}"
                )
                self._work_stack.fail(package.package_id, error)
                error_type = (
                    "oom"
                    if blast_result.exit_code == 137
                    else "diamond_error"
                )
                record_package_failed(error_type)
                return None, 0

            # Parse output and write as Parquet
            table = parse_output(raw_output)
            num_hits = table.num_rows
            result_parquet = (
                self._results_dir
                / f"{package.query_chunk_id}_{package.ref_chunk_id}.parquet"
            )
            pq.write_table(table, result_parquet)

            return result_parquet, num_hits

        finally:
            import shutil

            shutil.rmtree(work_dir, ignore_errors=True)

    def _get_or_build_ref_db(
        self,
        ref_chunk_id: str,
        ref_parquet: Path,
        work_dir: Path,
    ) -> Path | None:
        """Return path to a cached .dmnd file, building it if needed.

        Reference databases are cached in ``ref_dbs/`` so that multiple
        query chunks aligning against the same reference chunk reuse the
        same .dmnd file instead of rebuilding it each time.

        Args:
            ref_chunk_id: Reference chunk identifier.
            ref_parquet: Path to the reference chunk Parquet file.
            work_dir: Temp directory for intermediate files.

        Returns:
            Path to the .dmnd file, or None on build failure.
        """
        cached_db = self._ref_db_cache / f"{ref_chunk_id}.dmnd"

        if cached_db.exists():
            logger.debug(
                "ref_db_cache_hit",
                ref_chunk_id=ref_chunk_id,
                path=str(cached_db),
            )
            return cached_db

        # Cache miss — build the database
        logger.info(
            "ref_db_cache_miss",
            ref_chunk_id=ref_chunk_id,
        )

        ref_fasta = work_dir / "ref.fasta"
        parquet_chunk_to_fasta(ref_parquet, ref_fasta)

        # Build into cache directory (without .dmnd extension — DIAMOND adds it)
        db_stem = self._ref_db_cache / ref_chunk_id
        db_result = self._diamond.make_db(ref_fasta, db_stem)

        if db_result.exit_code != 0:
            logger.error(
                "ref_db_build_failed",
                ref_chunk_id=ref_chunk_id,
                error=db_result.error_message,
            )
            return None

        return cached_db

    def _find_chunk_parquet(self, chunk_id: str) -> Path | None:
        """Find a chunk Parquet file by chunk ID.

        Searches for ``chunk_{chunk_id}.parquet`` in the chunks directory
        and its subdirectories.

        Args:
            chunk_id: The chunk identifier.

        Returns:
            Path to the Parquet file, or None if not found.
        """
        candidates = list(
            self._chunks_dir.glob(f"**/chunk_{chunk_id}.parquet")
        )
        if candidates:
            return candidates[0]
        return None


def run_worker_process(
    work_stack_dir: Path,
    chunks_dir: Path,
    results_dir: Path,
    *,
    diamond_binary: str = "diamond",
    sensitivity: str = "very-sensitive",
    max_target_seqs: int = 50,
    timeout: int = 3600,
    heartbeat_interval: float = 30.0,
    heartbeat_timeout: int = 120,
    reaper_interval: float = 60.0,
    max_idle_time: float = 30.0,
    log_level: str = "INFO",
    run_id: str | None = None,
) -> int:
    """Entry point for a worker subprocess.

    Creates its own logging config, work stack connection,
    DiamondWrapper, and WorkerRunner. Designed to be called via
    ``multiprocessing.Process(target=run_worker_process, kwargs=...)``.

    Args:
        work_stack_dir: Path to the work stack directory.
        chunks_dir: Path to the chunks directory.
        results_dir: Path to the results directory.
        diamond_binary: DIAMOND binary path.
        sensitivity: DIAMOND sensitivity mode.
        max_target_seqs: Max hits per query.
        timeout: DIAMOND subprocess timeout.
        heartbeat_interval: Seconds between heartbeats.
        heartbeat_timeout: Seconds before stale.
        reaper_interval: Seconds between reaper scans.
        max_idle_time: Seconds idle before exit.
        log_level: Log level for this process.
        run_id: Pipeline run ID for log correlation.

    Returns:
        Number of packages completed.
    """
    from pathlib import Path as _Path

    from distributed_alignment.observability.logging import configure_logging
    from distributed_alignment.scheduler.filesystem_backend import (
        FileSystemWorkStack,
    )

    # Each process needs its own logging config (contextvars don't
    # propagate across process boundaries)
    configure_logging(level=log_level, run_id=run_id, json_output=True)

    stack = FileSystemWorkStack(_Path(work_stack_dir))
    diamond = DiamondWrapper(binary=diamond_binary, threads=1)

    runner = WorkerRunner(
        stack,
        diamond,
        _Path(chunks_dir),
        _Path(results_dir),
        sensitivity=sensitivity,
        max_target_seqs=max_target_seqs,
        timeout=timeout,
        heartbeat_interval=heartbeat_interval,
        heartbeat_timeout=heartbeat_timeout,
        reaper_interval=reaper_interval,
        max_idle_time=max_idle_time,
    )

    return runner.run()
