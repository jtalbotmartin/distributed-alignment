"""Tests for Stream C: ESM-2 embedding features.

Loader tests run unconditionally (no torch required).
Compute tests are skipped if torch/esm are not installed.
"""

from __future__ import annotations

import math
from pathlib import Path  # noqa: TCH003 — used at runtime in fixtures

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from distributed_alignment.features.embedding_features import (
    EMBEDDING_DIM,
    EMBEDDING_SCHEMA,
    load_embeddings,
)

FIXTURES_DIR = Path(__file__).parent / "fixtures"
FIXTURE_EMBEDDINGS = FIXTURES_DIR / "query_embeddings.parquet"
FIXTURE_FASTA = FIXTURES_DIR / "metagenome_queries.fasta"


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------


def _write_fake_embeddings(
    path: Path,
    *,
    n: int = 3,
    dim: int = EMBEDDING_DIM,
) -> None:
    """Write a minimal valid embeddings Parquet."""
    from datetime import UTC, datetime

    now = datetime.now(tz=UTC)
    ids = [f"SEQ{i:03d}" for i in range(n)]
    flat = [0.1] * (n * dim)

    table = pa.table(
        {
            "sequence_id": pa.array(ids, type=pa.string()),
            "embedding": pa.FixedSizeListArray.from_arrays(
                pa.array(flat, type=pa.float32()),
                list_size=dim,
            ),
            "feature_version": pa.array(["v1"] * n, type=pa.string()),
            "run_id": pa.array(["test"] * n, type=pa.string()),
            "created_at": pa.array([now] * n, type=pa.timestamp("us", tz="UTC")),
        },
        schema=EMBEDDING_SCHEMA,
    )
    pq.write_table(table, str(path))


# ---------------------------------------------------------------------------
# Loader tests — always run, no torch required
# ---------------------------------------------------------------------------


class TestLoadEmbeddings:
    def test_returns_table(self, tmp_path: Path) -> None:
        _write_fake_embeddings(tmp_path / "emb.parquet")
        table = load_embeddings(tmp_path / "emb.parquet")
        assert isinstance(table, pa.Table)

    def test_schema_matches(self, tmp_path: Path) -> None:
        _write_fake_embeddings(tmp_path / "emb.parquet")
        table = load_embeddings(tmp_path / "emb.parquet")
        assert table.schema.equals(EMBEDDING_SCHEMA)

    def test_embedding_dim(self, tmp_path: Path) -> None:
        _write_fake_embeddings(tmp_path / "emb.parquet")
        table = load_embeddings(tmp_path / "emb.parquet")
        vec = table.column("embedding")[0].as_py()
        assert len(vec) == EMBEDDING_DIM

    def test_sequence_ids_unique(self, tmp_path: Path) -> None:
        _write_fake_embeddings(tmp_path / "emb.parquet", n=5)
        table = load_embeddings(tmp_path / "emb.parquet")
        ids = table.column("sequence_id").to_pylist()
        assert len(ids) == len(set(ids))

    def test_missing_file(self) -> None:
        with pytest.raises(FileNotFoundError):
            load_embeddings(Path("/nonexistent/file.parquet"))

    def test_bad_schema_wrong_dim(self, tmp_path: Path) -> None:
        """Parquet with 128-dim embeddings should raise ValueError."""
        from datetime import UTC, datetime

        now = datetime.now(tz=UTC)
        table = pa.table(
            {
                "sequence_id": pa.array(["S1"], type=pa.string()),
                "embedding": pa.FixedSizeListArray.from_arrays(
                    pa.array([0.0] * 128, type=pa.float32()),
                    list_size=128,
                ),
                "feature_version": pa.array(["v1"], type=pa.string()),
                "run_id": pa.array(["test"], type=pa.string()),
                "created_at": pa.array(
                    [now],
                    type=pa.timestamp("us", tz="UTC"),
                ),
            }
        )
        path = tmp_path / "bad.parquet"
        pq.write_table(table, str(path))

        with pytest.raises(ValueError, match="128"):
            load_embeddings(path)


class TestLoadFixtureEmbeddings:
    """Tests against the committed fixture.

    Skipped if the fixture hasn't been generated yet.
    """

    @pytest.fixture(autouse=True)
    def _require_fixture(self) -> None:
        if not FIXTURE_EMBEDDINGS.exists():
            pytest.skip("Fixture not generated. Run: make compute-embeddings")

    def test_row_count(self) -> None:
        table = load_embeddings(FIXTURE_EMBEDDINGS)
        assert table.num_rows == 500

    def test_embedding_dim(self) -> None:
        table = load_embeddings(FIXTURE_EMBEDDINGS)
        vec = table.column("embedding")[0].as_py()
        assert len(vec) == EMBEDDING_DIM

    def test_sequence_ids_match_fasta(self) -> None:
        table = load_embeddings(FIXTURE_EMBEDDINGS)
        emb_ids = set(table.column("sequence_id").to_pylist())

        fasta_ids: set[str] = set()
        with FIXTURE_FASTA.open() as fh:
            for line in fh:
                if line.startswith(">"):
                    fasta_ids.add(line[1:].strip().split()[0])

        assert emb_ids == fasta_ids

    def test_values_are_finite(self) -> None:
        table = load_embeddings(FIXTURE_EMBEDDINGS)
        for row_idx in range(table.num_rows):
            vec = table.column("embedding")[row_idx].as_py()
            for val in vec:
                assert math.isfinite(val), f"Non-finite value in row {row_idx}"


# ---------------------------------------------------------------------------
# Compute script integration tests — skip if torch/esm unavailable
# ---------------------------------------------------------------------------

try:
    import esm as _esm  # noqa: F401
    import torch as _torch  # noqa: F401

    _has_esm = True
except ImportError:
    _has_esm = False


@pytest.mark.integration
@pytest.mark.skipif(not _has_esm, reason="torch/esm not installed")
class TestComputeEmbeddings:
    def _run_compute(
        self,
        fasta_path: Path,
        output_path: Path,
        batch_size: int = 4,
    ) -> pa.Table:
        """Run the compute function and return the output table."""
        from scripts.compute_embeddings import (
            _compute_embeddings,
        )

        sequences: list[tuple[str, str]] = []
        with fasta_path.open() as fh:
            header = ""
            seq_lines: list[str] = []
            for line in fh:
                if line.startswith(">"):
                    if header and seq_lines:
                        sid = header.split()[0]
                        sequences.append((sid, "".join(seq_lines)))
                    header = line[1:].strip()
                    seq_lines = []
                elif line.strip():
                    seq_lines.append(line.strip())
            if header and seq_lines:
                sid = header.split()[0]
                sequences.append((sid, "".join(seq_lines)))

        seq_ids, embeddings = _compute_embeddings(sequences, batch_size=batch_size)

        from datetime import UTC, datetime

        n = len(seq_ids)
        now = datetime.now(tz=UTC)
        table = pa.table(
            {
                "sequence_id": pa.array(seq_ids, type=pa.string()),
                "embedding": pa.FixedSizeListArray.from_arrays(
                    pa.array(
                        [v for vec in embeddings for v in vec],
                        type=pa.float32(),
                    ),
                    list_size=EMBEDDING_DIM,
                ),
                "feature_version": pa.array(["v1"] * n, type=pa.string()),
                "run_id": pa.array(["test"] * n, type=pa.string()),
                "created_at": pa.array(
                    [now] * n,
                    type=pa.timestamp("us", tz="UTC"),
                ),
            },
            schema=EMBEDDING_SCHEMA,
        )
        pq.write_table(table, str(output_path))
        return pq.read_table(str(output_path))

    def test_compute_small_fasta(self, tmp_path: Path) -> None:
        """Compute on a tiny FASTA → right shape and schema."""
        fasta = tmp_path / "tiny.fasta"
        fasta.write_text(
            ">SEQ1 test\nMKWVTFISLLFLFSSAYS\n"
            ">SEQ2 test\nACDEFGHIKLMNPQRSTVWY\n"
            ">SEQ3 test\nMMMMMMMMMM\n"
        )
        table = self._run_compute(fasta, tmp_path / "out.parquet")

        assert table.num_rows == 3
        assert table.schema.equals(EMBEDDING_SCHEMA)
        vec = table.column("embedding")[0].as_py()
        assert len(vec) == EMBEDDING_DIM

    def test_compute_deterministic(self, tmp_path: Path) -> None:
        """Two runs produce identical embeddings."""
        fasta = tmp_path / "det.fasta"
        fasta.write_text(
            ">S1 test\nACDEFGHIKLMNPQRSTVWY\n>S2 test\nMKWVTFISLLFLFSSAYS\n"
        )
        t1 = self._run_compute(fasta, tmp_path / "r1.parquet")
        t2 = self._run_compute(fasta, tmp_path / "r2.parquet")

        assert t1.column("embedding").to_pylist() == t2.column("embedding").to_pylist()

    def test_compute_batch_boundary(self, tmp_path: Path) -> None:
        """More sequences than batch_size → all included."""
        lines = []
        for i in range(7):
            lines.append(f">S{i} test\n")
            lines.append("ACDEFGHIKLMNPQRSTVWY\n")

        fasta = tmp_path / "batch.fasta"
        fasta.write_text("".join(lines))

        table = self._run_compute(
            fasta,
            tmp_path / "out.parquet",
            batch_size=3,
        )
        assert table.num_rows == 7
