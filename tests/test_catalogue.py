"""Tests for the data catalogue (Task 3.6)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path  # noqa: TCH003 — used at runtime

import pytest

from distributed_alignment.catalogue.store import CatalogueStore

# ---------------------------------------------------------------------------
# Initialisation and schema
# ---------------------------------------------------------------------------


class TestInitialisation:
    def test_fresh_catalogue_creates_tables(self, tmp_path: Path) -> None:
        cat = CatalogueStore(tmp_path / "cat.duckdb")
        # Can query all three tables without error
        cat._conn.execute("SELECT * FROM datasets LIMIT 0")
        cat._conn.execute("SELECT * FROM lineage LIMIT 0")
        cat._conn.execute("SELECT * FROM runs LIMIT 0")
        cat.close()

    def test_reopen_preserves_data(self, tmp_path: Path) -> None:
        path = tmp_path / "cat.duckdb"
        cat1 = CatalogueStore(path)
        cat1.register_dataset(
            "ds_001",
            "chunk",
            tmp_path / "chunk.parquet",
        )
        cat1.close()

        cat2 = CatalogueStore(path)
        ds = cat2.get_dataset("ds_001")
        assert ds is not None
        assert ds["dataset_id"] == "ds_001"
        cat2.close()

    def test_context_manager_closes_connection(self, tmp_path: Path) -> None:
        with CatalogueStore(tmp_path / "cat.duckdb") as cat:
            cat.register_dataset(
                "ds_001",
                "chunk",
                tmp_path / "c.parquet",
            )

        # Connection closed; queries on the closed store fail
        with pytest.raises((AttributeError, AssertionError)):
            cat.get_dataset("ds_001")

    def test_close_is_idempotent(self, tmp_path: Path) -> None:
        cat = CatalogueStore(tmp_path / "cat.duckdb")
        cat.close()
        cat.close()  # does not raise

    def test_parent_dirs_created(self, tmp_path: Path) -> None:
        deep = tmp_path / "a" / "b" / "c" / "cat.duckdb"
        cat = CatalogueStore(deep)
        assert deep.parent.exists()
        cat.close()


# ---------------------------------------------------------------------------
# Datasets
# ---------------------------------------------------------------------------


class TestDatasets:
    def test_register_and_get(self, tmp_path: Path) -> None:
        with CatalogueStore(tmp_path / "cat.duckdb") as cat:
            cat.register_dataset(
                "ds_merged_001",
                "merged",
                tmp_path / "merged.parquet",
                schema_version="v1",
                num_rows=12345,
                size_bytes=987654,
                content_checksum="sha256:abc",
                created_by_run="run_001",
                parameters={
                    "chunk_size": 1000,
                    "method": "deterministic_hash",
                },
            )
            rec = cat.get_dataset("ds_merged_001")

        assert rec is not None
        assert rec["dataset_type"] == "merged"
        assert rec["num_rows"] == 12345
        assert rec["size_bytes"] == 987654
        assert rec["content_checksum"] == "sha256:abc"
        assert rec["created_by_run"] == "run_001"
        assert rec["parameters"] == {
            "chunk_size": 1000,
            "method": "deterministic_hash",
        }

    def test_register_minimal(self, tmp_path: Path) -> None:
        with CatalogueStore(tmp_path / "cat.duckdb") as cat:
            cat.register_dataset(
                "ds_001",
                "chunk",
                tmp_path / "c.parquet",
            )
            rec = cat.get_dataset("ds_001")

        assert rec is not None
        assert rec["schema_version"] is None
        assert rec["num_rows"] is None
        assert rec["content_checksum"] is None
        assert rec["parameters"] is None

    def test_upsert(self, tmp_path: Path) -> None:
        with CatalogueStore(tmp_path / "cat.duckdb") as cat:
            cat.register_dataset(
                "ds_001",
                "chunk",
                tmp_path / "c.parquet",
                num_rows=100,
            )
            cat.register_dataset(
                "ds_001",
                "chunk",
                tmp_path / "c.parquet",
                num_rows=200,
            )
            rec = cat.get_dataset("ds_001")

        assert rec is not None
        assert rec["num_rows"] == 200

    def test_get_missing_returns_none(self, tmp_path: Path) -> None:
        with CatalogueStore(tmp_path / "cat.duckdb") as cat:
            assert cat.get_dataset("nonexistent") is None

    def test_parameters_roundtrip_as_dict(self, tmp_path: Path) -> None:
        with CatalogueStore(tmp_path / "cat.duckdb") as cat:
            cat.register_dataset(
                "ds_001",
                "chunk",
                tmp_path / "c.parquet",
                parameters={
                    "chunk_size": 1000,
                    "nested": {"k": "v"},
                    "list": [1, 2, 3],
                },
            )
            rec = cat.get_dataset("ds_001")

        assert rec is not None
        assert isinstance(rec["parameters"], dict)
        assert rec["parameters"]["nested"] == {"k": "v"}
        assert rec["parameters"]["list"] == [1, 2, 3]

    def test_list_filter_by_type(self, tmp_path: Path) -> None:
        with CatalogueStore(tmp_path / "cat.duckdb") as cat:
            cat.register_dataset("ds_001", "chunk", tmp_path / "a")
            cat.register_dataset("ds_002", "merged", tmp_path / "b")
            cat.register_dataset("ds_003", "chunk", tmp_path / "c")

            chunks = cat.list_datasets(dataset_type="chunk")
            merged = cat.list_datasets(dataset_type="merged")

        assert {r["dataset_id"] for r in chunks} == {
            "ds_001",
            "ds_003",
        }
        assert {r["dataset_id"] for r in merged} == {"ds_002"}

    def test_list_filter_by_run(self, tmp_path: Path) -> None:
        with CatalogueStore(tmp_path / "cat.duckdb") as cat:
            cat.register_dataset(
                "ds_001",
                "chunk",
                tmp_path / "a",
                created_by_run="run_A",
            )
            cat.register_dataset(
                "ds_002",
                "chunk",
                tmp_path / "b",
                created_by_run="run_B",
            )
            filtered = cat.list_datasets(created_by_run="run_A")

        assert len(filtered) == 1
        assert filtered[0]["dataset_id"] == "ds_001"

    def test_reference_dataset_has_null_run(self, tmp_path: Path) -> None:
        """Reference data (NCBI dump, UniProt) predates any run."""
        with CatalogueStore(tmp_path / "cat.duckdb") as cat:
            cat.register_dataset(
                "ds_ncbi_taxonomy",
                "reference",
                tmp_path / "taxdump",
                created_by_run=None,
            )
            rec = cat.get_dataset("ds_ncbi_taxonomy")

        assert rec is not None
        assert rec["created_by_run"] is None


# ---------------------------------------------------------------------------
# Runs
# ---------------------------------------------------------------------------


class TestRuns:
    def test_register_run_is_running(self, tmp_path: Path) -> None:
        with CatalogueStore(tmp_path / "cat.duckdb") as cat:
            cat.register_run("run_001", datetime.now(tz=UTC))
            rec = cat.get_run("run_001")

        assert rec is not None
        assert rec["status"] == "running"
        assert rec["completed_at"] is None

    def test_complete_run(self, tmp_path: Path) -> None:
        with CatalogueStore(tmp_path / "cat.duckdb") as cat:
            started = datetime.now(tz=UTC)
            cat.register_run("run_001", started)

            completed = started + timedelta(minutes=30)
            cat.complete_run(
                "run_001",
                completed,
                metrics={
                    "packages_processed": 500,
                    "duration_seconds": 1800,
                },
            )
            rec = cat.get_run("run_001")

        assert rec is not None
        assert rec["status"] == "completed"
        assert rec["completed_at"] is not None
        assert rec["metrics"]["packages_processed"] == 500

    def test_fail_run(self, tmp_path: Path) -> None:
        with CatalogueStore(tmp_path / "cat.duckdb") as cat:
            started = datetime.now(tz=UTC)
            cat.register_run("run_001", started)
            cat.fail_run(
                "run_001",
                started + timedelta(minutes=5),
                metrics={"error": "OOM"},
            )
            rec = cat.get_run("run_001")

        assert rec is not None
        assert rec["status"] == "failed"
        assert rec["metrics"]["error"] == "OOM"

    def test_config_and_metrics_roundtrip(self, tmp_path: Path) -> None:
        with CatalogueStore(tmp_path / "cat.duckdb") as cat:
            cat.register_run(
                "run_001",
                datetime.now(tz=UTC),
                config={
                    "num_workers": 4,
                    "backend": "multiprocess",
                },
            )
            cat.complete_run(
                "run_001",
                datetime.now(tz=UTC),
                metrics={"total_time": 120.5},
            )
            rec = cat.get_run("run_001")

        assert rec is not None
        assert isinstance(rec["config"], dict)
        assert isinstance(rec["metrics"], dict)
        assert rec["config"]["num_workers"] == 4

    def test_git_commit_optional(self, tmp_path: Path) -> None:
        with CatalogueStore(tmp_path / "cat.duckdb") as cat:
            cat.register_run("run_a", datetime.now(tz=UTC))
            cat.register_run(
                "run_b",
                datetime.now(tz=UTC),
                git_commit="abc123def",
            )

            a = cat.get_run("run_a")
            b = cat.get_run("run_b")

        assert a is not None
        assert b is not None
        assert a["git_commit"] is None
        assert b["git_commit"] == "abc123def"

    def test_list_runs_filter_by_status(self, tmp_path: Path) -> None:
        with CatalogueStore(tmp_path / "cat.duckdb") as cat:
            now = datetime.now(tz=UTC)
            cat.register_run("run_a", now)
            cat.register_run("run_b", now)
            cat.complete_run("run_b", now)
            cat.register_run("run_c", now)
            cat.fail_run("run_c", now)

            running = cat.list_runs(status="running")
            completed = cat.list_runs(status="completed")
            failed = cat.list_runs(status="failed")

        assert {r["run_id"] for r in running} == {"run_a"}
        assert {r["run_id"] for r in completed} == {"run_b"}
        assert {r["run_id"] for r in failed} == {"run_c"}


# ---------------------------------------------------------------------------
# Lineage
# ---------------------------------------------------------------------------


class TestLineage:
    def test_simple_edge(self, tmp_path: Path) -> None:
        with CatalogueStore(tmp_path / "cat.duckdb") as cat:
            cat.register_lineage("B", "A", "chunked_from")
            ancestors = cat.get_lineage("B")

        assert len(ancestors) == 1
        assert ancestors[0]["dataset_id"] == "A"
        assert ancestors[0]["depth"] == 1
        assert ancestors[0]["relationship"] == "chunked_from"

    def test_chain(self, tmp_path: Path) -> None:
        """A → B → C: get_lineage('C') returns B at depth 1, A at depth 2."""
        with CatalogueStore(tmp_path / "cat.duckdb") as cat:
            cat.register_lineage("B", "A", "chunked_from")
            cat.register_lineage("C", "B", "aligned_from")
            ancestors = cat.get_lineage("C")

        assert len(ancestors) == 2
        depths = {a["dataset_id"]: a["depth"] for a in ancestors}
        assert depths["B"] == 1
        assert depths["A"] == 2

    def test_descendants(self, tmp_path: Path) -> None:
        """A → B → C: get_lineage('A', direction='descendants') returns B, C."""
        with CatalogueStore(tmp_path / "cat.duckdb") as cat:
            cat.register_lineage("B", "A", "chunked_from")
            cat.register_lineage("C", "B", "aligned_from")
            descendants = cat.get_lineage("A", direction="descendants")

        depths = {d["dataset_id"]: d["depth"] for d in descendants}
        assert depths == {"B": 1, "C": 2}

    def test_branching(self, tmp_path: Path) -> None:
        """A → B and A → C: descendants of A returns both."""
        with CatalogueStore(tmp_path / "cat.duckdb") as cat:
            cat.register_lineage("B", "A", "chunked_from")
            cat.register_lineage("C", "A", "chunked_from")
            descendants = cat.get_lineage("A", direction="descendants")

        ids = {d["dataset_id"] for d in descendants}
        assert ids == {"B", "C"}

    def test_missing_dataset_returns_empty(self, tmp_path: Path) -> None:
        with CatalogueStore(tmp_path / "cat.duckdb") as cat:
            assert cat.get_lineage("nonexistent") == []

    def test_upsert(self, tmp_path: Path) -> None:
        with CatalogueStore(tmp_path / "cat.duckdb") as cat:
            cat.register_lineage("B", "A", "chunked_from")
            cat.register_lineage("B", "A", "aligned_from")
            ancestors = cat.get_lineage("B")

        assert len(ancestors) == 1
        assert ancestors[0]["relationship"] == "aligned_from"

    def test_max_depth(self, tmp_path: Path) -> None:
        """Chain of 5 with max_depth=2 returns only depth 1 and 2."""
        with CatalogueStore(tmp_path / "cat.duckdb") as cat:
            # E → D → C → B → A
            for parent, child in [
                ("A", "B"),
                ("B", "C"),
                ("C", "D"),
                ("D", "E"),
            ]:
                cat.register_lineage(child, parent, "chunked_from")

            limited = cat.get_lineage("E", max_depth=2)

        depths = {r["depth"] for r in limited}
        assert depths == {1, 2}
        assert len(limited) == 2


# ---------------------------------------------------------------------------
# FK-ish behaviour
# ---------------------------------------------------------------------------


class TestForeignKeyBehaviour:
    def test_lineage_with_unknown_datasets_allowed(self, tmp_path: Path) -> None:
        """
        Lineage edges may reference datasets not yet registered.
        Pipeline stages often register edges before all datasets
        are committed. Referential integrity is the caller's
        responsibility; the catalogue is a thin recorder.
        """
        with CatalogueStore(tmp_path / "cat.duckdb") as cat:
            # Neither dataset registered
            cat.register_lineage(
                "child_that_doesnt_exist",
                "parent_that_doesnt_exist",
                "chunked_from",
            )
            ancestors = cat.get_lineage("child_that_doesnt_exist")

        assert len(ancestors) == 1
        assert ancestors[0]["dataset_id"] == "parent_that_doesnt_exist"
