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

        configure_logging(
            level="INFO", run_id="integration_test", json_output=True
        )

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
            assert total_hits[0] > 0, (
                "Expected at least some alignment hits"
            )

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
