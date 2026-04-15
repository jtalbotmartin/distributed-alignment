"""Tests for Prometheus metrics and the dual backend abstraction."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from prometheus_client import REGISTRY

from distributed_alignment.observability.metrics import (
    PrometheusMetrics,
    get_metrics,
    record_diamond_result,
    record_package_completed,
    record_package_failed,
    reset_metrics,
    start_metrics_server,
    update_package_states,
)

if TYPE_CHECKING:
    from pathlib import Path


@pytest.fixture(autouse=True)
def _reset_metrics_singleton() -> None:
    """Reset the metrics singleton between tests."""
    reset_metrics()


class TestMetricDefinitions:
    """Verify all metrics are correctly defined."""

    def test_all_metrics_registered(self) -> None:
        """All expected metrics exist in the registry."""
        # Force creation of PrometheusMetrics
        get_metrics()

        names = {m.name for m in REGISTRY.collect() if m.name.startswith("da_")}
        assert "da_packages_total" in names
        assert "da_package_duration_seconds" in names
        assert "da_sequences_processed" in names
        assert "da_hits_found" in names
        assert "da_worker_count" in names
        assert "da_errors" in names
        assert "da_diamond_exit_code" in names


class TestBackendAutoDetection:
    """Tests for the get_metrics() auto-detection."""

    def test_returns_prometheus_by_default(self) -> None:
        m = get_metrics()
        assert isinstance(m, PrometheusMetrics)

    def test_singleton_returns_same_instance(self) -> None:
        m1 = get_metrics()
        m2 = get_metrics()
        assert m1 is m2

    def test_reset_clears_singleton(self) -> None:
        m1 = get_metrics()
        reset_metrics()
        m2 = get_metrics()
        assert m1 is not m2


class TestRecordPackageCompleted:
    """Tests for the record_package_completed helper."""

    def test_updates_histogram(self) -> None:
        record_package_completed(duration_seconds=5.5, num_sequences=100, num_hits=50)

        sample = REGISTRY.get_sample_value("da_package_duration_seconds_sum")
        assert sample is not None
        assert sample >= 5.5

    def test_updates_sequence_counter(self) -> None:
        record_package_completed(duration_seconds=1.0, num_sequences=200, num_hits=10)

        sample = REGISTRY.get_sample_value("da_sequences_processed_total")
        assert sample is not None
        assert sample >= 200

    def test_updates_hit_counter(self) -> None:
        record_package_completed(duration_seconds=1.0, num_sequences=10, num_hits=75)

        sample = REGISTRY.get_sample_value("da_hits_found_total")
        assert sample is not None
        assert sample >= 75


class TestRecordPackageFailed:
    """Tests for the record_package_failed helper."""

    def test_increments_error_counter(self) -> None:
        record_package_failed("oom")

        sample = REGISTRY.get_sample_value(
            "da_errors_total",
            {"error_type": "oom"},
        )
        assert sample is not None
        assert sample >= 1

    def test_different_error_types(self) -> None:
        record_package_failed("timeout")
        record_package_failed("diamond_error")

        timeout_val = REGISTRY.get_sample_value(
            "da_errors_total",
            {"error_type": "timeout"},
        )
        diamond_val = REGISTRY.get_sample_value(
            "da_errors_total",
            {"error_type": "diamond_error"},
        )
        assert timeout_val is not None and timeout_val >= 1
        assert diamond_val is not None and diamond_val >= 1


class TestRecordDiamondResult:
    """Tests for the record_diamond_result helper."""

    def test_records_exit_code(self) -> None:
        record_diamond_result(0)

        sample = REGISTRY.get_sample_value(
            "da_diamond_exit_code_total",
            {"exit_code": "0"},
        )
        assert sample is not None
        assert sample >= 1

    def test_records_oom_exit_code(self) -> None:
        record_diamond_result(137)

        sample = REGISTRY.get_sample_value(
            "da_diamond_exit_code_total",
            {"exit_code": "137"},
        )
        assert sample is not None
        assert sample >= 1


class TestUpdatePackageStates:
    """Tests for the update_package_states helper."""

    def test_sets_gauge_values(self) -> None:
        update_package_states(
            {
                "PENDING": 5,
                "RUNNING": 2,
                "COMPLETED": 10,
                "POISONED": 1,
            }
        )

        assert REGISTRY.get_sample_value("da_packages_total", {"state": "PENDING"}) == 5
        assert (
            REGISTRY.get_sample_value("da_packages_total", {"state": "COMPLETED"}) == 10
        )

    def test_updates_overwrite_previous(self) -> None:
        update_package_states({"PENDING": 10, "COMPLETED": 0})
        update_package_states({"PENDING": 3, "COMPLETED": 7})

        assert REGISTRY.get_sample_value("da_packages_total", {"state": "PENDING"}) == 3


class TestMetricsServer:
    """Tests for the metrics HTTP server."""

    def test_start_metrics_server(self) -> None:
        import urllib.request

        started = start_metrics_server(port=19091)
        assert started

        resp = urllib.request.urlopen(  # noqa: S310
            "http://localhost:19091/metrics"
        )
        body = resp.read().decode()
        assert resp.status == 200
        assert "da_packages_total" in body

    def test_port_already_in_use(self) -> None:
        started = start_metrics_server(port=19091)
        assert started is False


class TestWorkerRunnerMetrics:
    """Integration: metrics emitted during package processing."""

    def test_metrics_updated_after_processing(self, tmp_path: Path) -> None:
        from datetime import UTC, datetime
        from unittest.mock import MagicMock

        from distributed_alignment.ingest.chunker import (
            chunk_sequences,
        )
        from distributed_alignment.models import (
            ChunkEntry,
            ChunkManifest,
            ProteinSequence,
        )
        from distributed_alignment.scheduler.filesystem_backend import (
            FileSystemWorkStack,
        )
        from distributed_alignment.worker.diamond_wrapper import (
            DiamondResult,
            DiamondWrapper,
        )
        from distributed_alignment.worker.runner import WorkerRunner

        amino = "ACDEFGHIKLMNPQRSTVWY"
        seqs = [
            ProteinSequence(
                id=f"seq_{i:04d}",
                description=f"seq_{i:04d} test",
                sequence=amino * 3,
                length=60,
            )
            for i in range(5)
        ]
        chunks_dir = tmp_path / "chunks"
        chunk_sequences(
            seqs,
            num_chunks=1,
            output_dir=chunks_dir / "queries",
            chunk_prefix="q",
            run_id="metrics_test",
            input_files=["q.fasta"],
        )
        chunk_sequences(
            seqs,
            num_chunks=1,
            output_dir=chunks_dir / "refs",
            chunk_prefix="r",
            run_id="metrics_test",
            input_files=["r.fasta"],
        )

        now = datetime.now(tz=UTC)
        q = ChunkManifest(
            run_id="metrics_test",
            input_files=["q.fasta"],
            total_sequences=5,
            num_chunks=1,
            chunk_size_target=5,
            chunks=[
                ChunkEntry(
                    chunk_id="q000",
                    num_sequences=5,
                    parquet_path="fake",
                    content_checksum="sha256:fake",
                )
            ],
            created_at=now,
            chunking_strategy="deterministic_hash",
        )
        r = ChunkManifest(
            run_id="metrics_test",
            input_files=["r.fasta"],
            total_sequences=5,
            num_chunks=1,
            chunk_size_target=5,
            chunks=[
                ChunkEntry(
                    chunk_id="r000",
                    num_sequences=5,
                    parquet_path="fake",
                    content_checksum="sha256:fake",
                )
            ],
            created_at=now,
            chunking_strategy="deterministic_hash",
        )

        stack = FileSystemWorkStack(tmp_path / "work")
        stack.generate_work_packages(q, r)

        mock = MagicMock(spec=DiamondWrapper)
        mock.make_db.return_value = DiamondResult(
            exit_code=0, duration_seconds=0.01, stderr=""
        )

        from pathlib import Path as _Path

        def fake_blastp(
            query_fasta: object,
            ref_db: object,
            output_path: _Path,
            **kwargs: object,
        ) -> DiamondResult:
            _Path(str(output_path)).write_text(
                "seq_0001\tref_0001\t85.0\t100\t15\t0\t1\t100\t1\t100\t1e-30\t200.0\n"
            )
            return DiamondResult(
                exit_code=0,
                duration_seconds=0.1,
                stderr="",
                output_path=str(output_path),
            )

        mock.run_blastp.side_effect = fake_blastp

        runner = WorkerRunner(
            stack,
            mock,  # type: ignore[arg-type]
            chunks_dir,
            tmp_path / "results",
            max_idle_time=1.0,
        )
        completed = runner.run()

        assert completed == 1

        # Verify metrics
        duration_sum = REGISTRY.get_sample_value("da_package_duration_seconds_sum")
        assert duration_sum is not None and duration_sum > 0

        diamond_ok = REGISTRY.get_sample_value(
            "da_diamond_exit_code_total",
            {"exit_code": "0"},
        )
        assert diamond_ok is not None and diamond_ok >= 1
