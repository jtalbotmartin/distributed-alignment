"""Clean interface around the DIAMOND binary for protein alignment.

Handles subprocess management, output parsing, exit code interpretation,
and timeout handling. Application code uses this wrapper rather than
calling DIAMOND directly.
"""

from __future__ import annotations

import shutil
import subprocess
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import pyarrow as pa
import structlog

if TYPE_CHECKING:
    from pathlib import Path

logger = structlog.get_logger(component="diamond")

# DIAMOND format 6 column names and PyArrow types
DIAMOND_COLUMNS: list[tuple[str, pa.DataType]] = [
    ("qseqid", pa.string()),
    ("sseqid", pa.string()),
    ("pident", pa.float64()),
    ("length", pa.int32()),
    ("mismatch", pa.int32()),
    ("gapopen", pa.int32()),
    ("qstart", pa.int32()),
    ("qend", pa.int32()),
    ("sstart", pa.int32()),
    ("send", pa.int32()),
    ("evalue", pa.float64()),
    ("bitscore", pa.float64()),
]

DIAMOND_SCHEMA = pa.schema([pa.field(name, dtype) for name, dtype in DIAMOND_COLUMNS])


@dataclass
class DiamondResult:
    """Result of a DIAMOND execution."""

    exit_code: int
    duration_seconds: float
    stderr: str
    output_path: str | None = None
    error_message: str | None = None


@dataclass
class DiamondWrapper:
    """Wrapper around the DIAMOND binary.

    Args:
        binary: Path or name of the DIAMOND binary.
        threads: Number of threads for DIAMOND to use.
    """

    binary: str = "diamond"
    threads: int = 1
    extra_args: list[str] = field(default_factory=list)

    def check_available(self) -> bool:
        """Check whether the DIAMOND binary is available on the system.

        Returns:
            True if the binary can be executed, False otherwise.
        """
        if shutil.which(self.binary) is None:
            logger.warning(
                "diamond_not_found",
                binary=self.binary,
            )
            return False

        try:
            result = subprocess.run(
                [self.binary, "version"],
                capture_output=True,
                text=True,
                timeout=10,
                check=False,
            )
            if result.returncode == 0:
                logger.info(
                    "diamond_available",
                    binary=self.binary,
                    version=result.stdout.strip(),
                )
                return True
        except (subprocess.TimeoutExpired, OSError):
            pass

        logger.warning(
            "diamond_check_failed",
            binary=self.binary,
        )
        return False

    def make_db(
        self,
        fasta_path: Path,
        db_path: Path,
        *,
        timeout: int = 600,
    ) -> DiamondResult:
        """Build a DIAMOND database from a FASTA file.

        Args:
            fasta_path: Path to input FASTA file.
            db_path: Path for the output .dmnd database (without extension).
            timeout: Subprocess timeout in seconds.

        Returns:
            DiamondResult with exit code and timing.
        """
        cmd = [
            self.binary,
            "makedb",
            "--in",
            str(fasta_path),
            "--db",
            str(db_path),
            "--threads",
            str(self.threads),
        ]

        logger.info(
            "diamond_makedb_start",
            fasta_path=str(fasta_path),
            db_path=str(db_path),
        )

        return self._run_command(cmd, timeout=timeout)

    def run_blastp(
        self,
        query_fasta: Path,
        ref_db: Path,
        output_path: Path,
        *,
        sensitivity: str = "very-sensitive",
        max_target_seqs: int = 50,
        timeout: int = 3600,
    ) -> DiamondResult:
        """Run DIAMOND blastp alignment.

        Args:
            query_fasta: Path to query FASTA file.
            ref_db: Path to reference DIAMOND database (.dmnd).
            output_path: Path to write tab-separated output.
            sensitivity: DIAMOND sensitivity mode.
            max_target_seqs: Maximum target sequences per query.
            timeout: Subprocess timeout in seconds.

        Returns:
            DiamondResult with exit code, timing, and output path.
        """
        cmd = [
            self.binary,
            "blastp",
            "--db",
            str(ref_db),
            "--query",
            str(query_fasta),
            "--out",
            str(output_path),
            "--outfmt",
            "6",
            "--threads",
            str(self.threads),
            f"--{sensitivity}",
            "--max-target-seqs",
            str(max_target_seqs),
            *self.extra_args,
        ]

        logger.info(
            "diamond_blastp_start",
            query=str(query_fasta),
            ref_db=str(ref_db),
            sensitivity=sensitivity,
        )

        result = self._run_command(cmd, timeout=timeout)
        if result.exit_code == 0:
            result.output_path = str(output_path)
        return result

    def _run_command(self, cmd: list[str], *, timeout: int) -> DiamondResult:
        """Execute a DIAMOND command as a subprocess.

        Args:
            cmd: Command and arguments.
            timeout: Subprocess timeout in seconds.

        Returns:
            DiamondResult with exit code, timing, and error info.
        """
        start = time.monotonic()

        try:
            proc = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=timeout,
                check=False,
            )
        except subprocess.TimeoutExpired:
            duration = time.monotonic() - start
            msg = f"DIAMOND timed out after {timeout}s"
            logger.error(
                "diamond_timeout",
                command=cmd[1],
                timeout=timeout,
                duration=duration,
            )
            return DiamondResult(
                exit_code=-1,
                duration_seconds=duration,
                stderr="",
                error_message=msg,
            )
        except FileNotFoundError:
            duration = time.monotonic() - start
            msg = (
                f"DIAMOND binary not found: '{self.binary}'. "
                f"Is DIAMOND installed and on your PATH?"
            )
            logger.error(
                "diamond_not_found",
                binary=self.binary,
            )
            return DiamondResult(
                exit_code=-2,
                duration_seconds=duration,
                stderr="",
                error_message=msg,
            )

        duration = time.monotonic() - start

        result = DiamondResult(
            exit_code=proc.returncode,
            duration_seconds=duration,
            stderr=proc.stderr.strip(),
        )

        if proc.returncode == 0:
            logger.info(
                "diamond_success",
                command=cmd[1],
                duration=round(duration, 2),
            )
        elif proc.returncode == 137:
            result.error_message = (
                "DIAMOND killed by OOM (exit code 137). Try increasing worker memory."
            )
            logger.error(
                "diamond_oom",
                command=cmd[1],
                exit_code=137,
                stderr=proc.stderr.strip(),
            )
        else:
            result.error_message = (
                f"DIAMOND failed with exit code {proc.returncode}: "
                f"{proc.stderr.strip()}"
            )
            logger.error(
                "diamond_failed",
                command=cmd[1],
                exit_code=proc.returncode,
                stderr=proc.stderr.strip(),
            )

        return result


def parse_output(output_path: Path) -> pa.Table:
    """Parse DIAMOND tab-separated format 6 output into a PyArrow Table.

    Args:
        output_path: Path to the DIAMOND output file.

    Returns:
        PyArrow Table with typed columns matching DIAMOND_SCHEMA.
        Empty table with correct schema if the file is empty.
    """
    import csv

    columns: dict[str, list[str | float | int]] = {
        name: [] for name, _ in DIAMOND_COLUMNS
    }

    with output_path.open() as f:
        reader = csv.reader(f, delimiter="\t")
        for row in reader:
            if not row or row[0].startswith("#"):
                continue
            for i, (name, _) in enumerate(DIAMOND_COLUMNS):
                columns[name].append(row[i])

    # Build typed arrays
    arrays: list[pa.Array] = []
    for name, dtype in DIAMOND_COLUMNS:
        raw = columns[name]
        if pa.types.is_string(dtype):
            arrays.append(pa.array(raw, type=pa.string()))
        elif pa.types.is_float64(dtype):
            arrays.append(pa.array([float(v) for v in raw], type=dtype))
        elif pa.types.is_int32(dtype):
            arrays.append(pa.array([int(v) for v in raw], type=dtype))
        else:
            arrays.append(pa.array(raw))

    return pa.table(arrays, schema=DIAMOND_SCHEMA)
