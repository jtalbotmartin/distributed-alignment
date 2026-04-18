"""End-to-end integration test for the full Phase 1 pipeline.

Exercises: ingest → chunk → schedule → align → merge → query results.
Requires DIAMOND binary — marked @pytest.mark.integration.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import duckdb
import pyarrow.parquet as pq
import pytest

from distributed_alignment.ingest.chunker import chunk_sequences
from distributed_alignment.ingest.fasta_parser import parse_fasta
from distributed_alignment.merge.merger import MERGED_SCHEMA, merge_query_chunk
from distributed_alignment.observability.logging import configure_logging
from distributed_alignment.scheduler.filesystem_backend import (
    FileSystemWorkStack,
)
from distributed_alignment.worker.diamond_wrapper import DiamondWrapper
from distributed_alignment.worker.runner import WorkerRunner

if TYPE_CHECKING:
    from pathlib import Path


@pytest.mark.integration
class TestFullPipeline:
    """End-to-end pipeline test using real DIAMOND and Swiss-Prot data."""

    def test_ingest_schedule_align_merge(
        self,
        integration_test_data: tuple[Path, Path],
        tmp_path: Path,
    ) -> None:
        """Full pipeline: ingest → chunk → schedule → align → merge → query."""
        diamond = DiamondWrapper(binary="diamond", threads=1)
        if not diamond.check_available():
            pytest.skip("DIAMOND binary not available")

        configure_logging(level="INFO", run_id="integration_test", json_output=True)

        queries_fasta, ref_fasta = integration_test_data
        work_dir = tmp_path / "work"

        # --- Step 1: Ingest and chunk ---
        q_seqs = list(parse_fasta(queries_fasta))
        r_seqs = list(parse_fasta(ref_fasta))

        assert len(q_seqs) > 0
        assert len(r_seqs) > 0

        # Use 2 query chunks and 2 ref chunks to test the Cartesian product
        q_num_chunks = min(2, len(q_seqs))
        r_num_chunks = min(2, len(r_seqs))

        q_manifest = chunk_sequences(
            q_seqs,
            num_chunks=q_num_chunks,
            output_dir=work_dir / "chunks" / "queries",
            chunk_prefix="q",
            run_id="integration_test",
            input_files=[str(queries_fasta)],
        )
        r_manifest = chunk_sequences(
            r_seqs,
            num_chunks=r_num_chunks,
            output_dir=work_dir / "chunks" / "references",
            chunk_prefix="r",
            run_id="integration_test",
            input_files=[str(ref_fasta)],
        )

        # Write manifests (as the CLI would)
        q_manifest_path = work_dir / "query_manifest.json"
        r_manifest_path = work_dir / "ref_manifest.json"
        q_manifest_path.write_text(
            json.dumps(q_manifest.model_dump(mode="json"), indent=2)
        )
        r_manifest_path.write_text(
            json.dumps(r_manifest.model_dump(mode="json"), indent=2)
        )

        expected_packages = len(q_manifest.chunks) * len(r_manifest.chunks)

        # --- Step 2: Schedule ---
        stack = FileSystemWorkStack(work_dir / "work_stack")
        packages = stack.generate_work_packages(q_manifest, r_manifest)

        assert len(packages) == expected_packages
        assert stack.pending_count() == expected_packages

        # --- Step 3: Align ---
        results_dir = work_dir / "results"
        runner = WorkerRunner(
            stack,
            diamond,
            work_dir / "chunks",
            results_dir,
            sensitivity="fast",
            max_target_seqs=10,
            timeout=120,
        )
        completed = runner.run()

        assert completed == expected_packages
        assert stack.pending_count() == 0
        assert stack.status()["COMPLETED"] == expected_packages
        assert stack.status()["POISONED"] == 0

        # --- Step 4: Merge ---
        merged_dir = work_dir / "merged"
        ref_chunk_ids = [c.chunk_id for c in r_manifest.chunks]

        for q_chunk in q_manifest.chunks:
            merge_query_chunk(
                q_chunk.chunk_id,
                results_dir,
                merged_dir,
                top_n=10,
                expected_ref_chunks=ref_chunk_ids,
            )

        # --- Step 5: Verify ---
        merged_files = list(merged_dir.glob("merged_*.parquet"))
        assert len(merged_files) == len(q_manifest.chunks)

        # Schema check
        for f in merged_files:
            table = pq.read_table(f)
            assert table.schema.equals(MERGED_SCHEMA)

        # Queryable via DuckDB
        merged_pattern = str(merged_dir / "merged_*.parquet")
        con = duckdb.connect()
        try:
            total_hits = con.execute(
                f"SELECT count(*) FROM read_parquet('{merged_pattern}')"
            ).fetchone()
            assert total_hits is not None
            assert total_hits[0] > 0, "Expected at least some alignment hits"

            # Check biological meaningfulness with Swiss-Prot data
            best_evalue = con.execute(
                f"SELECT min(evalue) FROM read_parquet('{merged_pattern}')"
            ).fetchone()
            assert best_evalue is not None
            if best_evalue[0] is not None:
                assert best_evalue[0] < 1e-5, (
                    "Expected at least one strong hit (evalue < 1e-5) "
                    "between human and E. coli proteins"
                )

            # Verify global ranking is correct (rank 1 has best evalue per query)
            rank_check = con.execute(
                f"""
                SELECT query_id, global_rank, evalue
                FROM read_parquet('{merged_pattern}')
                WHERE global_rank = 1
                ORDER BY evalue
                LIMIT 5
                """
            ).fetchall()
            assert len(rank_check) > 0
        finally:
            con.close()


# ---------------------------------------------------------------------------
# Phase 3 feature-pipeline integration tests
#
# These don't require DIAMOND — they start from a synthetic merged
# Parquet and exercise enrich → features → combine → catalogue.
# ---------------------------------------------------------------------------

from datetime import UTC, datetime  # noqa: E402
from pathlib import Path  # noqa: E402

import pyarrow as pa  # noqa: E402

from distributed_alignment.catalogue import CatalogueStore  # noqa: E402
from distributed_alignment.features.embedding_features import (  # noqa: E402
    EMBEDDING_DIM,
    EMBEDDING_SCHEMA,
)
from distributed_alignment.ingest.chunker import CHUNK_SCHEMA  # noqa: E402
from distributed_alignment.pipeline import (  # noqa: E402
    RunCollisionError,
    run_feature_pipeline,
)
from distributed_alignment.taxonomy import TaxonomyDB  # noqa: E402

_FIXTURES = Path(__file__).parent / "fixtures"
_TAX_FIXTURES = _FIXTURES / "taxonomy"


def _write_phase3_chunks(chunks_dir: Path, sequences: list[tuple[str, str]]) -> None:
    """Write one chunk file with the given (sequence_id, sequence) pairs."""
    query_dir = chunks_dir / "queries"
    query_dir.mkdir(parents=True, exist_ok=True)
    n = len(sequences)
    ids = [s[0] for s in sequences]
    seqs = [s[1] for s in sequences]
    table = pa.table(
        {
            "chunk_id": pa.array(["q000"] * n, type=pa.string()),
            "sequence_id": pa.array(ids, type=pa.string()),
            "description": pa.array([f">{s}" for s in ids], type=pa.string()),
            "sequence": pa.array(seqs, type=pa.string()),
            "length": pa.array([len(s) for s in seqs], type=pa.int32()),
            "content_hash": pa.array(["demo"] * n, type=pa.string()),
        },
        schema=CHUNK_SCHEMA,
    )
    pq.write_table(table, str(query_dir / "chunk_q000.parquet"))


def _write_synthetic_merged(
    path: Path, hits: list[tuple[str, str, float, int, float, int]]
) -> None:
    """Write a synthetic merged Parquet from hit tuples.

    Each tuple: (query_id, subject_id, pident, align_len, evalue, rank).
    """
    rows = []
    for query_id, subject_id, pident, align_len, evalue, grank in hits:
        rows.append(
            {
                "query_id": query_id,
                "subject_id": subject_id,
                "percent_identity": pident,
                "alignment_length": align_len,
                "mismatches": 5,
                "gap_opens": 0,
                "query_start": 1,
                "query_end": align_len,
                "subject_start": 1,
                "subject_end": align_len,
                "evalue": evalue,
                "bitscore": 200.0,
                "global_rank": grank,
                "query_chunk_id": "q000",
                "ref_chunk_id": "r000",
            }
        )
    arrays: dict[str, list[object]] = {f.name: [] for f in MERGED_SCHEMA}
    for row in rows:
        for name in arrays:
            arrays[name].append(row[name])
    table = pa.table(
        {
            name: pa.array(vals, type=field.type)
            for (name, vals), field in zip(arrays.items(), MERGED_SCHEMA, strict=True)
        },
    )
    pq.write_table(table, str(path))


def _write_synthetic_embeddings(path: Path, seq_ids: list[str]) -> None:
    n = len(seq_ids)
    flat = [0.1] * (n * EMBEDDING_DIM)
    table = pa.table(
        {
            "sequence_id": pa.array(seq_ids, type=pa.string()),
            "embedding": pa.FixedSizeListArray.from_arrays(
                pa.array(flat, type=pa.float32()),
                list_size=EMBEDDING_DIM,
            ),
            "feature_version": pa.array(["v1"] * n, type=pa.string()),
            "run_id": pa.array(["test"] * n, type=pa.string()),
            "created_at": pa.array(
                [datetime.now(tz=UTC)] * n,
                type=pa.timestamp("us", tz="UTC"),
            ),
        },
        schema=EMBEDDING_SCHEMA,
    )
    pq.write_table(table, str(path))


@pytest.fixture()
def phase3_env(tmp_path: Path) -> dict[str, Path]:
    """Build a fresh Phase 3 test environment.

    Returns paths for chunks, merged, taxonomy, embeddings, and
    the (not-yet-created) features_dir and catalogue_path.
    """
    chunks_dir = tmp_path / "chunks"
    _write_phase3_chunks(
        chunks_dir,
        [
            ("Q_1", "MKWVTFISLLFLFSSAYSMKWVTFISLLFLFSSAYS"),
            ("Q_2", "ACDEFGHIKLMNPQRSTVWYACDEFGHIKLMNPQRSTVWY"),
            ("Q_3", "MMMMMMMMMMACDEFGHIKLMNPQRST"),
            ("Q_dark", "AAACCCGGGTTTTTTAAACCCGGG"),
        ],
    )

    merged_path = tmp_path / "merged.parquet"
    _write_synthetic_merged(
        merged_path,
        [
            ("Q_1", "sp|P0A8M3|SYT_ECOLI", 92.5, 100, 1e-50, 1),
            ("Q_2", "sp|C0SP85|YUKE_BACSU", 88.1, 120, 1e-45, 1),
            ("Q_3", "sp|D6VTK4|YAR1_YEAST", 65.0, 80, 1e-15, 1),
        ],
    )

    # Build a taxonomy DB from the committed fixtures
    tax_db_path = tmp_path / "taxonomy.duckdb"
    TaxonomyDB.from_ncbi_dump(
        nodes_path=_TAX_FIXTURES / "nodes.dmp",
        names_path=_TAX_FIXTURES / "names.dmp",
        accession2taxid_path=_TAX_FIXTURES / "accession2taxid.tsv",
        db_path=tax_db_path,
    )

    embeddings_path = tmp_path / "embeddings.parquet"
    _write_synthetic_embeddings(embeddings_path, ["Q_1", "Q_2", "Q_3"])

    return {
        "chunks_dir": chunks_dir,
        "merged_path": merged_path,
        "tax_db_path": tax_db_path,
        "embeddings_path": embeddings_path,
        "features_dir": tmp_path / "features",
        "catalogue_path": tmp_path / "catalogue.duckdb",
    }


# ---------------------------------------------------------------------------
# Coverage matrix
# ---------------------------------------------------------------------------


class TestPipelineCoverageMatrix:
    """Each combination of --taxonomy-db / --embeddings / --skip-* flags."""

    def test_without_taxonomy_or_embeddings(self, phase3_env: dict[str, Path]) -> None:
        result = run_feature_pipeline(
            run_id="run_basic",
            merged_parquet_path=phase3_env["merged_path"],
            chunks_dir=phase3_env["chunks_dir"],
            features_dir=phase3_env["features_dir"],
            catalogue_path=phase3_env["catalogue_path"],
        )

        assert result.status == "completed"
        assert "enriched" not in result.outputs
        assert "alignment_features" in result.outputs
        assert "kmer_features" in result.outputs
        assert "combined_features" in result.outputs

        combined = pq.read_table(str(result.outputs["combined_features"]))
        assert "esm2_embedding" not in combined.schema.names

    def test_with_taxonomy_no_embeddings(self, phase3_env: dict[str, Path]) -> None:
        result = run_feature_pipeline(
            run_id="run_tax",
            merged_parquet_path=phase3_env["merged_path"],
            chunks_dir=phase3_env["chunks_dir"],
            features_dir=phase3_env["features_dir"],
            catalogue_path=phase3_env["catalogue_path"],
            taxonomy_db_path=phase3_env["tax_db_path"],
        )

        assert result.status == "completed"
        assert "enriched" in result.outputs

        # Alignment features should have populated taxonomic columns
        af = pq.read_table(str(result.outputs["alignment_features"]))
        # Q_1 maps to E. coli → Pseudomonadota
        row = {name: af.column(name).to_pylist() for name in af.schema.names}
        idx = row["sequence_id"].index("Q_1")
        assert row["num_phyla"][idx] == 1

        combined = pq.read_table(str(result.outputs["combined_features"]))
        assert "esm2_embedding" not in combined.schema.names

    def test_with_all_streams(self, phase3_env: dict[str, Path]) -> None:
        result = run_feature_pipeline(
            run_id="run_all",
            merged_parquet_path=phase3_env["merged_path"],
            chunks_dir=phase3_env["chunks_dir"],
            features_dir=phase3_env["features_dir"],
            catalogue_path=phase3_env["catalogue_path"],
            taxonomy_db_path=phase3_env["tax_db_path"],
            embeddings_path=phase3_env["embeddings_path"],
        )

        assert result.status == "completed"
        combined = pq.read_table(str(result.outputs["combined_features"]))
        assert "esm2_embedding" in combined.schema.names

    def test_skip_enrichment(self, phase3_env: dict[str, Path]) -> None:
        result = run_feature_pipeline(
            run_id="run_skip_enrich",
            merged_parquet_path=phase3_env["merged_path"],
            chunks_dir=phase3_env["chunks_dir"],
            features_dir=phase3_env["features_dir"],
            catalogue_path=phase3_env["catalogue_path"],
            taxonomy_db_path=phase3_env["tax_db_path"],
            skip_enrichment=True,
        )

        assert result.status == "completed"
        assert "enriched" not in result.outputs
        assert "alignment_features" in result.outputs

    def test_skip_features(self, phase3_env: dict[str, Path]) -> None:
        result = run_feature_pipeline(
            run_id="run_skip_feat",
            merged_parquet_path=phase3_env["merged_path"],
            chunks_dir=phase3_env["chunks_dir"],
            features_dir=phase3_env["features_dir"],
            catalogue_path=phase3_env["catalogue_path"],
            taxonomy_db_path=phase3_env["tax_db_path"],
            skip_features=True,
        )

        assert result.status == "completed"
        assert "enriched" in result.outputs
        assert "alignment_features" not in result.outputs
        assert "combined_features" not in result.outputs


# ---------------------------------------------------------------------------
# Catalogue integration
# ---------------------------------------------------------------------------


class TestCatalogueIntegration:
    def test_run_status_lifecycle(self, phase3_env: dict[str, Path]) -> None:
        run_feature_pipeline(
            run_id="run_lifecycle",
            merged_parquet_path=phase3_env["merged_path"],
            chunks_dir=phase3_env["chunks_dir"],
            features_dir=phase3_env["features_dir"],
            catalogue_path=phase3_env["catalogue_path"],
        )

        with CatalogueStore(phase3_env["catalogue_path"]) as cat:
            run = cat.get_run("run_lifecycle")
        assert run is not None
        assert run["status"] == "completed"
        assert run["completed_at"] is not None

    def test_lineage_chain(self, phase3_env: dict[str, Path]) -> None:
        run_feature_pipeline(
            run_id="run_lin",
            merged_parquet_path=phase3_env["merged_path"],
            chunks_dir=phase3_env["chunks_dir"],
            features_dir=phase3_env["features_dir"],
            catalogue_path=phase3_env["catalogue_path"],
            taxonomy_db_path=phase3_env["tax_db_path"],
        )

        with CatalogueStore(phase3_env["catalogue_path"]) as cat:
            ancestors = cat.get_lineage(
                "combined_features_run_lin", direction="ancestors"
            )

        ancestor_ids = {a["dataset_id"] for a in ancestors}
        assert "alignment_features_run_lin" in ancestor_ids
        assert "kmer_features_run_lin" in ancestor_ids
        assert "enriched_run_lin" in ancestor_ids
        assert "merged_run_lin" in ancestor_ids

    def test_embeddings_registered_as_reference(
        self, phase3_env: dict[str, Path]
    ) -> None:
        run_feature_pipeline(
            run_id="run_emb",
            merged_parquet_path=phase3_env["merged_path"],
            chunks_dir=phase3_env["chunks_dir"],
            features_dir=phase3_env["features_dir"],
            catalogue_path=phase3_env["catalogue_path"],
            embeddings_path=phase3_env["embeddings_path"],
        )

        with CatalogueStore(phase3_env["catalogue_path"]) as cat:
            emb = cat.get_dataset("embeddings_run_emb")
        assert emb is not None
        assert emb["created_by_run"] is None

    def test_catalogue_created_on_first_run(self, phase3_env: dict[str, Path]) -> None:
        cat_path = phase3_env["catalogue_path"]
        assert not cat_path.exists()
        run_feature_pipeline(
            run_id="run_new",
            merged_parquet_path=phase3_env["merged_path"],
            chunks_dir=phase3_env["chunks_dir"],
            features_dir=phase3_env["features_dir"],
            catalogue_path=cat_path,
        )
        assert cat_path.exists()


# ---------------------------------------------------------------------------
# Rerun semantics
# ---------------------------------------------------------------------------


class TestRerunSemantics:
    def test_rerun_same_id_collides(self, phase3_env: dict[str, Path]) -> None:
        run_feature_pipeline(
            run_id="run_once",
            merged_parquet_path=phase3_env["merged_path"],
            chunks_dir=phase3_env["chunks_dir"],
            features_dir=phase3_env["features_dir"],
            catalogue_path=phase3_env["catalogue_path"],
        )

        with pytest.raises(RunCollisionError):
            run_feature_pipeline(
                run_id="run_once",
                merged_parquet_path=phase3_env["merged_path"],
                chunks_dir=phase3_env["chunks_dir"],
                features_dir=phase3_env["features_dir"],
                catalogue_path=phase3_env["catalogue_path"],
            )

    def test_force_rerun_is_idempotent(self, phase3_env: dict[str, Path]) -> None:
        for _ in range(2):
            run_feature_pipeline(
                run_id="run_idem",
                merged_parquet_path=phase3_env["merged_path"],
                chunks_dir=phase3_env["chunks_dir"],
                features_dir=phase3_env["features_dir"],
                catalogue_path=phase3_env["catalogue_path"],
                force_rerun=True,
            )

        # Upsert semantics: second run doesn't accumulate duplicates.
        # Datasets without enrichment/embeddings:
        # merged + chunks_queries + alignment + kmer + combined = 5
        with CatalogueStore(phase3_env["catalogue_path"]) as cat:
            datasets = cat.list_datasets()
            assert len(datasets) == 5
            lineage = cat.get_lineage(
                "combined_features_run_idem", direction="ancestors"
            )
        assert len(lineage) >= 2


# ---------------------------------------------------------------------------
# Failure path
# ---------------------------------------------------------------------------


class TestFailurePath:
    def test_run_failure_marks_run_failed(
        self, phase3_env: dict[str, Path], tmp_path: Path
    ) -> None:
        # Point --taxonomy-db at a non-existent file
        bad_tax_path = tmp_path / "nonexistent.duckdb"

        with pytest.raises(FileNotFoundError):
            run_feature_pipeline(
                run_id="run_fail",
                merged_parquet_path=phase3_env["merged_path"],
                chunks_dir=phase3_env["chunks_dir"],
                features_dir=phase3_env["features_dir"],
                catalogue_path=phase3_env["catalogue_path"],
                taxonomy_db_path=bad_tax_path,
            )

        with CatalogueStore(phase3_env["catalogue_path"]) as cat:
            run = cat.get_run("run_fail")
        assert run is not None
        assert run["status"] == "failed"
        assert "error" in run["metrics"]

        # Combined output should not exist (failed before features)
        combined_path = (
            phase3_env["features_dir"] / "run_fail" / "combined_features.parquet"
        )
        assert not combined_path.exists()


# ---------------------------------------------------------------------------
# CLI smoke
# ---------------------------------------------------------------------------


class TestCliSmoke:
    def test_cli_run_command_accepts_phase3_flags(self) -> None:
        """Typer should parse the new flags without error."""
        from typer.testing import CliRunner

        from distributed_alignment.cli import app

        runner = CliRunner()
        result = runner.invoke(app, ["run", "--help"])
        assert result.exit_code == 0
        # Phase 3 flags should be documented in help output
        for flag in (
            "--taxonomy-db",
            "--embeddings",
            "--skip-enrichment",
            "--skip-features",
            "--feature-version",
            "--run-id",
            "--git-commit",
            "--force-rerun",
        ):
            assert flag in result.output
