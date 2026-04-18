"""Data catalogue — metadata store for pipeline datasets, lineage, and runs.

Three tables backed by DuckDB:

- ``datasets``: where Parquet outputs live, what produced them,
  schema version, row counts, checksums.
- ``lineage``: parent → child relationships between datasets, walked
  via a recursive CTE to return full ancestry or descendants.
- ``runs``: pipeline invocations with start/end time, status, config,
  and optional git commit.

The catalogue is single-writer, multi-reader — DuckDB's file lock
enforces this.  Each writer should open its own :class:`CatalogueStore`.

All timestamps are stored as ``TIMESTAMP WITH TIME ZONE`` (requires the
``pytz`` dependency at bind time).  Callers should pass tz-aware
datetimes (typically ``datetime.now(tz=UTC)``).
"""

from __future__ import annotations

import json
from datetime import datetime  # noqa: TCH003 — used at runtime
from pathlib import Path  # noqa: TCH003 — used at runtime
from types import TracebackType  # noqa: TCH003 — used at runtime
from typing import Any, Literal

import duckdb
import structlog

logger = structlog.get_logger(__name__)


def _jsonify(value: dict[str, Any] | None) -> str | None:
    """Serialise a dict for a DuckDB JSON column.

    Uses ``default=str`` so non-JSON-serialisable values (datetimes,
    Paths) survive as their string representation.  The catalogue is
    for inspection, not precise round-tripping.
    """
    if value is None:
        return None
    return json.dumps(value, default=str)


def _parse_json(value: str | None) -> dict[str, Any] | None:
    """Deserialise a DuckDB JSON column back into a dict."""
    if value is None:
        return None
    return json.loads(value)


class CatalogueStore:
    """DuckDB-backed catalogue of pipeline datasets, lineage, and runs.

    Open with a path; tables are created if they don't exist.  Use
    as a context manager for automatic cleanup::

        with CatalogueStore(path) as cat:
            cat.register_dataset(...)

    Single-writer only.  Multiple concurrent writers will conflict on
    DuckDB's file lock; open a separate :class:`CatalogueStore` per
    writer process.
    """

    def __init__(self, path: Path) -> None:
        """Open or create the catalogue at *path*.

        Creates parent directories if needed.  Creates the three
        tables if they don't already exist.
        """
        path.parent.mkdir(parents=True, exist_ok=True)
        self._path = path
        self._conn: duckdb.DuckDBPyConnection | None = duckdb.connect(str(path))
        self._create_tables()
        logger.info("catalogue_opened", path=str(path))

    def _create_tables(self) -> None:
        assert self._conn is not None
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS datasets (
                dataset_id TEXT PRIMARY KEY,
                dataset_type TEXT NOT NULL,
                path TEXT NOT NULL,
                schema_version TEXT,
                num_rows BIGINT,
                size_bytes BIGINT,
                content_checksum TEXT,
                created_at TIMESTAMP WITH TIME ZONE NOT NULL,
                created_by_run TEXT,
                parameters JSON
            )
            """
        )
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS lineage (
                child_dataset_id TEXT NOT NULL,
                parent_dataset_id TEXT NOT NULL,
                relationship TEXT NOT NULL,
                PRIMARY KEY (child_dataset_id, parent_dataset_id)
            )
            """
        )
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS runs (
                run_id TEXT PRIMARY KEY,
                started_at TIMESTAMP WITH TIME ZONE NOT NULL,
                completed_at TIMESTAMP WITH TIME ZONE,
                status TEXT NOT NULL,
                config JSON,
                metrics JSON,
                git_commit TEXT
            )
            """
        )

    # -- Datasets -----------------------------------------------------------

    def register_dataset(
        self,
        dataset_id: str,
        dataset_type: str,
        path: Path,
        *,
        schema_version: str | None = None,
        num_rows: int | None = None,
        size_bytes: int | None = None,
        content_checksum: str | None = None,
        created_by_run: str | None = None,
        parameters: dict[str, Any] | None = None,
    ) -> None:
        """Insert or update a dataset record (upsert on ``dataset_id``).

        Reference datasets that predate any pipeline run should pass
        ``created_by_run=None``.
        """
        from datetime import UTC

        now = datetime.now(tz=UTC)
        assert self._conn is not None
        self._conn.execute(
            """
            INSERT INTO datasets (
                dataset_id, dataset_type, path, schema_version,
                num_rows, size_bytes, content_checksum,
                created_at, created_by_run, parameters
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (dataset_id) DO UPDATE SET
                dataset_type = excluded.dataset_type,
                path = excluded.path,
                schema_version = excluded.schema_version,
                num_rows = excluded.num_rows,
                size_bytes = excluded.size_bytes,
                content_checksum = excluded.content_checksum,
                created_at = excluded.created_at,
                created_by_run = excluded.created_by_run,
                parameters = excluded.parameters
            """,
            [
                dataset_id,
                dataset_type,
                str(path),
                schema_version,
                num_rows,
                size_bytes,
                content_checksum,
                now,
                created_by_run,
                _jsonify(parameters),
            ],
        )

    def get_dataset(self, dataset_id: str) -> dict[str, Any] | None:
        """Return the dataset record as a dict, or ``None`` if missing."""
        assert self._conn is not None
        row = self._conn.execute(
            "SELECT * FROM datasets WHERE dataset_id = ?",
            [dataset_id],
        ).fetchone()
        if row is None:
            return None
        cols = [
            d[0]
            for d in self._conn.execute("SELECT * FROM datasets LIMIT 0").description
        ]
        record = dict(zip(cols, row, strict=True))
        record["parameters"] = _parse_json(record["parameters"])
        return record

    def list_datasets(
        self,
        *,
        dataset_type: str | None = None,
        created_by_run: str | None = None,
    ) -> list[dict[str, Any]]:
        """List datasets, optionally filtered by type and/or run."""
        assert self._conn is not None
        clauses: list[str] = []
        params: list[Any] = []
        if dataset_type is not None:
            clauses.append("dataset_type = ?")
            params.append(dataset_type)
        if created_by_run is not None:
            clauses.append("created_by_run = ?")
            params.append(created_by_run)

        sql = "SELECT * FROM datasets"
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY created_at DESC"

        rows = self._conn.execute(sql, params).fetchall()
        cols = [
            d[0]
            for d in self._conn.execute("SELECT * FROM datasets LIMIT 0").description
        ]
        records: list[dict[str, Any]] = []
        for row in rows:
            rec = dict(zip(cols, row, strict=True))
            rec["parameters"] = _parse_json(rec["parameters"])
            records.append(rec)
        return records

    # -- Runs ---------------------------------------------------------------

    def register_run(
        self,
        run_id: str,
        started_at: datetime,
        *,
        config: dict[str, Any] | None = None,
        git_commit: str | None = None,
    ) -> None:
        """Start a new run (status='running') or update ``started_at``.

        Upsert on ``run_id``.  Does not touch ``completed_at`` or
        ``status`` — those are managed by :meth:`complete_run` and
        :meth:`fail_run`.
        """
        assert self._conn is not None
        self._conn.execute(
            """
            INSERT INTO runs (
                run_id, started_at, completed_at, status,
                config, metrics, git_commit
            )
            VALUES (?, ?, NULL, 'running', ?, NULL, ?)
            ON CONFLICT (run_id) DO UPDATE SET
                started_at = excluded.started_at,
                config = excluded.config,
                git_commit = excluded.git_commit
            """,
            [
                run_id,
                started_at,
                _jsonify(config),
                git_commit,
            ],
        )

    def complete_run(
        self,
        run_id: str,
        completed_at: datetime,
        *,
        metrics: dict[str, Any] | None = None,
    ) -> None:
        """Mark a run complete."""
        self._finish_run(run_id, completed_at, "completed", metrics)

    def fail_run(
        self,
        run_id: str,
        completed_at: datetime,
        *,
        metrics: dict[str, Any] | None = None,
    ) -> None:
        """Mark a run failed."""
        self._finish_run(run_id, completed_at, "failed", metrics)

    def _finish_run(
        self,
        run_id: str,
        completed_at: datetime,
        status: str,
        metrics: dict[str, Any] | None,
    ) -> None:
        assert self._conn is not None
        self._conn.execute(
            """
            UPDATE runs SET
                completed_at = ?,
                status = ?,
                metrics = ?
            WHERE run_id = ?
            """,
            [
                completed_at,
                status,
                _jsonify(metrics),
                run_id,
            ],
        )

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        """Return the run record as a dict, or ``None`` if missing."""
        assert self._conn is not None
        row = self._conn.execute(
            "SELECT * FROM runs WHERE run_id = ?",
            [run_id],
        ).fetchone()
        if row is None:
            return None
        cols = [
            d[0] for d in self._conn.execute("SELECT * FROM runs LIMIT 0").description
        ]
        rec = dict(zip(cols, row, strict=True))
        rec["config"] = _parse_json(rec["config"])
        rec["metrics"] = _parse_json(rec["metrics"])
        return rec

    def list_runs(self, *, status: str | None = None) -> list[dict[str, Any]]:
        """List runs, optionally filtered by status."""
        assert self._conn is not None
        sql = "SELECT * FROM runs"
        params: list[Any] = []
        if status is not None:
            sql += " WHERE status = ?"
            params.append(status)
        sql += " ORDER BY started_at DESC"

        rows = self._conn.execute(sql, params).fetchall()
        cols = [
            d[0] for d in self._conn.execute("SELECT * FROM runs LIMIT 0").description
        ]
        records: list[dict[str, Any]] = []
        for row in rows:
            rec = dict(zip(cols, row, strict=True))
            rec["config"] = _parse_json(rec["config"])
            rec["metrics"] = _parse_json(rec["metrics"])
            records.append(rec)
        return records

    # -- Lineage ------------------------------------------------------------

    def register_lineage(
        self,
        child_dataset_id: str,
        parent_dataset_id: str,
        relationship: str,
    ) -> None:
        """Upsert a lineage edge.

        Does **not** validate that either dataset exists — pipeline
        stages may register edges before both datasets are committed.
        Enforce referential integrity at the application layer if
        needed.
        """
        assert self._conn is not None
        self._conn.execute(
            """
            INSERT INTO lineage (
                child_dataset_id, parent_dataset_id, relationship
            )
            VALUES (?, ?, ?)
            ON CONFLICT (child_dataset_id, parent_dataset_id)
                DO UPDATE SET relationship = excluded.relationship
            """,
            [child_dataset_id, parent_dataset_id, relationship],
        )

    def get_lineage(
        self,
        dataset_id: str,
        *,
        direction: Literal["ancestors", "descendants"] = "ancestors",
        max_depth: int = 10,
    ) -> list[dict[str, Any]]:
        """Walk the lineage graph from *dataset_id*.

        Args:
            dataset_id: Starting dataset.
            direction: ``"ancestors"`` (what produced this) or
                ``"descendants"`` (what depends on this).
            max_depth: Safety cap against pathological cycles.

        Returns:
            List of dicts with ``dataset_id``, ``relationship``,
            ``depth``, ordered by depth.
        """
        assert self._conn is not None
        if direction == "ancestors":
            sql = """
                WITH RECURSIVE walk(
                    dataset_id, relationship, depth
                ) AS (
                    SELECT
                        parent_dataset_id AS dataset_id,
                        relationship,
                        1 AS depth
                    FROM lineage
                    WHERE child_dataset_id = ?
                    UNION ALL
                    SELECT
                        l.parent_dataset_id,
                        l.relationship,
                        w.depth + 1
                    FROM lineage l
                    JOIN walk w
                        ON l.child_dataset_id = w.dataset_id
                    WHERE w.depth < ?
                )
                SELECT dataset_id, relationship, depth
                FROM walk
                ORDER BY depth, dataset_id
            """
        else:  # descendants
            sql = """
                WITH RECURSIVE walk(
                    dataset_id, relationship, depth
                ) AS (
                    SELECT
                        child_dataset_id AS dataset_id,
                        relationship,
                        1 AS depth
                    FROM lineage
                    WHERE parent_dataset_id = ?
                    UNION ALL
                    SELECT
                        l.child_dataset_id,
                        l.relationship,
                        w.depth + 1
                    FROM lineage l
                    JOIN walk w
                        ON l.parent_dataset_id = w.dataset_id
                    WHERE w.depth < ?
                )
                SELECT dataset_id, relationship, depth
                FROM walk
                ORDER BY depth, dataset_id
            """

        rows = self._conn.execute(sql, [dataset_id, max_depth]).fetchall()
        return [
            {
                "dataset_id": r[0],
                "relationship": r[1],
                "depth": r[2],
            }
            for r in rows
        ]

    # -- Lifecycle ----------------------------------------------------------

    def close(self) -> None:
        """Close the DuckDB connection. Idempotent."""
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def __enter__(self) -> CatalogueStore:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.close()
