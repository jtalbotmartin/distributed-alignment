"""Worker runner — main loop for claiming and processing work packages.

Claims packages from a WorkStack, executes DIAMOND alignment via
DiamondWrapper, writes result Parquet, and marks packages complete.
"""

from __future__ import annotations

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

    from distributed_alignment.models import WorkPackage
    from distributed_alignment.scheduler.filesystem_backend import (
        FileSystemWorkStack,
    )

logger = structlog.get_logger()


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


class WorkerRunner:
    """Main worker loop: claim → align → write → complete → repeat.

    Args:
        work_stack: WorkStack to claim packages from.
        diamond: DiamondWrapper for executing alignment.
        chunks_dir: Directory containing chunk Parquet files.
        results_dir: Directory to write result Parquet files.
        sensitivity: DIAMOND sensitivity mode.
        max_target_seqs: Maximum target sequences per query.
        timeout: DIAMOND subprocess timeout in seconds.
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
    ) -> None:
        self._work_stack = work_stack
        self._diamond = diamond
        self._chunks_dir = chunks_dir
        self._results_dir = results_dir
        self._sensitivity = sensitivity
        self._max_target_seqs = max_target_seqs
        self._timeout = timeout
        self._worker_id = f"worker-{uuid.uuid4().hex[:8]}"

        self._results_dir.mkdir(parents=True, exist_ok=True)

    @property
    def worker_id(self) -> str:
        """Return this worker's unique identifier."""
        return self._worker_id

    def run(self) -> int:
        """Run the worker loop until no pending packages remain.

        Returns:
            Number of packages successfully processed.
        """
        completed = 0

        logger.info(
            "worker_started",
            worker_id=self._worker_id,
        )

        while True:
            package = self._work_stack.claim(self._worker_id)
            if package is None:
                break

            success = self._process_package(package)
            if success:
                completed += 1

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
        log = logger.bind(
            package_id=package.package_id,
            worker_id=self._worker_id,
            query_chunk=package.query_chunk_id,
            ref_chunk=package.ref_chunk_id,
        )
        log.info("processing_package")

        try:
            result_path = self._run_alignment(package)
        except Exception as exc:
            error_msg = f"{type(exc).__name__}: {exc}"
            log.error("package_failed", error=error_msg)
            self._work_stack.fail(package.package_id, error_msg)
            return False

        if result_path is None:
            return False

        self._work_stack.complete(
            package.package_id, str(result_path)
        )
        log.info("package_completed", result_path=str(result_path))
        return True

    def _run_alignment(self, package: WorkPackage) -> Path | None:
        """Execute the alignment for a work package.

        Converts Parquet chunks to FASTA, builds the reference DB if
        needed, runs DIAMOND blastp, and writes results as Parquet.

        Args:
            package: The work package to process.

        Returns:
            Path to the result Parquet file, or None on failure.
        """
        from pathlib import Path as _Path

        # Locate chunk Parquet files
        query_parquet = self._find_chunk_parquet(package.query_chunk_id)
        ref_parquet = self._find_chunk_parquet(package.ref_chunk_id)

        if query_parquet is None or ref_parquet is None:
            error = (
                f"Missing chunk file: "
                f"query={query_parquet}, ref={ref_parquet}"
            )
            self._work_stack.fail(package.package_id, error)
            return None

        # Create temp working directory for this package
        work_dir = self._results_dir / f".tmp_{package.package_id}"
        work_dir.mkdir(parents=True, exist_ok=True)

        try:
            # Convert Parquet → FASTA for DIAMOND
            query_fasta = work_dir / "query.fasta"
            ref_fasta = work_dir / "ref.fasta"
            parquet_chunk_to_fasta(query_parquet, query_fasta)
            parquet_chunk_to_fasta(ref_parquet, ref_fasta)

            # Build reference DB
            ref_db = work_dir / "ref"
            db_result = self._diamond.make_db(ref_fasta, ref_db)
            if db_result.exit_code != 0:
                error = (
                    db_result.error_message
                    or f"makedb failed: exit {db_result.exit_code}"
                )
                self._work_stack.fail(package.package_id, error)
                return None

            # Run alignment
            raw_output = work_dir / "output.tsv"
            blast_result = self._diamond.run_blastp(
                query_fasta,
                _Path(f"{ref_db}.dmnd"),
                raw_output,
                sensitivity=self._sensitivity,
                max_target_seqs=self._max_target_seqs,
                timeout=self._timeout,
            )

            if blast_result.exit_code != 0:
                error = (
                    blast_result.error_message
                    or f"blastp failed: exit {blast_result.exit_code}"
                )
                self._work_stack.fail(package.package_id, error)
                return None

            # Parse output and write as Parquet
            table = parse_output(raw_output)
            result_parquet = (
                self._results_dir
                / f"{package.query_chunk_id}_{package.ref_chunk_id}.parquet"
            )
            pq.write_table(table, result_parquet)

            return result_parquet

        finally:
            # Clean up temp directory
            import shutil

            shutil.rmtree(work_dir, ignore_errors=True)

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
