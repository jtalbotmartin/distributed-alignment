"""Tests for Ray integration.

All tests require Ray and are marked @pytest.mark.integration.
Skipped automatically if Ray is not installed.
"""

from __future__ import annotations

import time
from datetime import UTC, datetime
from pathlib import Path  # noqa: TCH003 — used at runtime for Path.cwd()
from unittest.mock import patch

import pytest

ray = pytest.importorskip("ray")

# Ray has issues with paths containing spaces (e.g. iCloud Drive).
# Skip integration tests if the working directory path has spaces.
_HAS_SPACE_IN_PATH = " " in str(Path.cwd())

from distributed_alignment.ingest.chunker import chunk_sequences  # noqa: E402
from distributed_alignment.models import (  # noqa: E402
    ChunkEntry,
    ChunkManifest,
    ProteinSequence,
)
from distributed_alignment.scheduler.filesystem_backend import (  # noqa: E402
    FileSystemWorkStack,
)
from distributed_alignment.worker.ray_actor import (  # noqa: E402
    _try_import_ray,
    run_ray_workers,
)

AMINO = "ACDEFGHIKLMNPQRSTVWY"


@pytest.fixture(autouse=True)
def _clear_ray_hook() -> None:
    """Remove Ray's uv runtime env hook before each test.

    When tests run via ``uv run pytest``, uv sets
    ``RAY_RUNTIME_ENV_HOOK`` which interferes with ``ray.init()``.
    Must be cleared before Ray starts.
    """
    import os

    os.environ.pop("RAY_RUNTIME_ENV_HOOK", None)


def _make_sequences(n: int) -> list[ProteinSequence]:
    return [
        ProteinSequence(
            id=f"seq_{i:04d}",
            description=f"seq_{i:04d} test",
            sequence=AMINO * 3,
            length=len(AMINO) * 3,
        )
        for i in range(n)
    ]


def _make_manifests(
    num_q: int = 2,
    num_r: int = 1,
) -> tuple[ChunkManifest, ChunkManifest]:
    now = datetime.now(tz=UTC)
    q = ChunkManifest(
        run_id="ray_test",
        input_files=["q.fasta"],
        total_sequences=num_q * 10,
        num_chunks=num_q,
        chunk_size_target=10,
        chunks=[
            ChunkEntry(
                chunk_id=f"q{i:03d}",
                num_sequences=10,
                parquet_path=f"chunks/queries/chunk_q{i:03d}.parquet",
                content_checksum="sha256:fake",
            )
            for i in range(num_q)
        ],
        created_at=now,
        chunking_strategy="deterministic_hash",
    )
    r = ChunkManifest(
        run_id="ray_test",
        input_files=["r.fasta"],
        total_sequences=num_r * 10,
        num_chunks=num_r,
        chunk_size_target=10,
        chunks=[
            ChunkEntry(
                chunk_id=f"r{i:03d}",
                num_sequences=10,
                parquet_path=f"chunks/refs/chunk_r{i:03d}.parquet",
                content_checksum="sha256:fake",
            )
            for i in range(num_r)
        ],
        created_at=now,
        chunking_strategy="deterministic_hash",
    )
    return q, r


def _setup_work(
    tmp_path: Path,
    num_q: int = 2,
    num_r: int = 1,
) -> tuple[FileSystemWorkStack, Path, Path]:
    chunks_dir = tmp_path / "chunks"
    seqs = _make_sequences(10)
    chunk_sequences(
        seqs,
        num_chunks=num_q,
        output_dir=chunks_dir / "queries",
        chunk_prefix="q",
        run_id="ray_test",
        input_files=["q.fasta"],
    )
    chunk_sequences(
        seqs,
        num_chunks=num_r,
        output_dir=chunks_dir / "refs",
        chunk_prefix="r",
        run_id="ray_test",
        input_files=["r.fasta"],
    )
    stack = FileSystemWorkStack(tmp_path / "work")
    q, r = _make_manifests(num_q, num_r)
    stack.generate_work_packages(q, r)
    return stack, chunks_dir, tmp_path / "results"


def _check_diamond() -> bool:
    """Check if real DIAMOND binary is available."""
    from distributed_alignment.worker.diamond_wrapper import (
        DiamondWrapper,
    )

    return DiamondWrapper(binary="diamond").check_available()


@pytest.mark.integration
@pytest.mark.skipif(
    _HAS_SPACE_IN_PATH,
    reason="Ray working directory packaging fails on paths with spaces",
)
class TestRayBasicFunctionality:
    """Test that Ray actors process work packages correctly.

    Uses real DIAMOND — can't mock inside Ray actor processes.
    """

    def test_ray_workers_complete_all_packages(
        self,
        tmp_path: Path,
    ) -> None:
        """2 Ray actors processing 4 packages → all 4 completed."""
        if not _check_diamond():
            pytest.skip("DIAMOND not available")

        stack, chunks_dir, results_dir = _setup_work(
            tmp_path,
            num_q=4,
            num_r=1,
        )

        config = {
            "work_stack_dir": str(tmp_path / "work"),
            "chunks_dir": str(chunks_dir),
            "results_dir": str(results_dir),
            "diamond_binary": "diamond",
            "sensitivity": "fast",
            "max_target_seqs": 5,
            "timeout": 30,
            "heartbeat_interval": 0.5,
            "heartbeat_timeout": 5,
            "reaper_interval": 1.0,
            "max_idle_time": 5.0,
            "log_level": "WARNING",
            "run_id": "ray_test",
        }

        results = run_ray_workers(config, num_workers=2)

        total_completed = sum(r.get("packages_completed", 0) for r in results)
        errors = [r for r in results if r.get("error")]

        assert total_completed == 4, (
            f"Expected 4 completed, got {total_completed}. Errors: {errors}"
        )
        assert stack.status()["COMPLETED"] == 4
        assert stack.status()["POISONED"] == 0

    def test_ray_concurrent_execution(
        self,
        tmp_path: Path,
    ) -> None:
        """Verify 2 actors are faster than 1 (parallel execution)."""
        if not _check_diamond():
            pytest.skip("DIAMOND not available")

        stack, chunks_dir, results_dir = _setup_work(
            tmp_path,
            num_q=4,
            num_r=1,
        )

        config = {
            "work_stack_dir": str(tmp_path / "work"),
            "chunks_dir": str(chunks_dir),
            "results_dir": str(results_dir),
            "diamond_binary": "diamond",
            "sensitivity": "fast",
            "max_target_seqs": 5,
            "timeout": 30,
            "heartbeat_interval": 0.5,
            "heartbeat_timeout": 5,
            "reaper_interval": 1.0,
            "max_idle_time": 5.0,
            "log_level": "WARNING",
            "run_id": "ray_parallel_test",
        }

        start = time.monotonic()
        results = run_ray_workers(config, num_workers=2)
        elapsed = time.monotonic() - start

        total = sum(r.get("packages_completed", 0) for r in results)
        assert total == 4
        # Soft timing check — shouldn't take more than 60s
        assert elapsed < 60, f"Took {elapsed}s"


@pytest.mark.integration
@pytest.mark.skipif(
    _HAS_SPACE_IN_PATH,
    reason="Ray working directory packaging fails on paths with spaces",
)
class TestRayErrorHandling:
    """Test that Ray actors handle errors gracefully."""

    def test_actor_with_bad_config_returns_error(
        self,
        tmp_path: Path,
    ) -> None:
        """Actor with nonexistent paths returns error, doesn't crash."""
        config = {
            "work_stack_dir": str(tmp_path / "nonexistent"),
            "chunks_dir": str(tmp_path / "nope"),
            "results_dir": str(tmp_path / "nope2"),
            "diamond_binary": "/nonexistent/diamond",
            "max_idle_time": 1.0,
            "log_level": "ERROR",
        }

        results = run_ray_workers(config, num_workers=1)

        assert len(results) == 1
        assert isinstance(results[0], dict)


class TestRayImportHandling:
    """Test handling when Ray is/isn't installed."""

    def test_try_import_ray_succeeds(self) -> None:
        result = _try_import_ray()
        assert result is not None

    def test_try_import_ray_failure_message(self) -> None:
        with (
            patch.dict("sys.modules", {"ray": None}),
            pytest.raises(
                ImportError,
                match="Ray is not installed",
            ),
        ):
            _try_import_ray()


class TestBackendFlag:
    """Test CLI --backend flag handling."""

    def test_backend_local_accepted(self) -> None:
        from typer.testing import CliRunner

        from distributed_alignment.cli import app

        runner = CliRunner()
        result = runner.invoke(app, ["run", "--help"])
        assert "--backend" in result.output

    def test_backend_ray_import_error(self) -> None:
        with (
            patch.dict("sys.modules", {"ray": None}),
            pytest.raises(
                ImportError,
                match="Ray is not installed",
            ),
        ):
            _try_import_ray()
