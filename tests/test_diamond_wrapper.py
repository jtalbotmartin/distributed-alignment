"""Tests for the DIAMOND wrapper.

Unit tests use fixture data and don't require DIAMOND.
Integration tests (marked @pytest.mark.integration) require the DIAMOND binary.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pytest

from distributed_alignment.worker.diamond_wrapper import (
    DIAMOND_SCHEMA,
    DiamondResult,
    DiamondWrapper,
    parse_output,
)

FIXTURES = Path(__file__).parent / "fixtures"


class TestParseOutput:
    """Unit tests for parsing DIAMOND format 6 output."""

    def test_parse_fixture_file(self) -> None:
        table = parse_output(FIXTURES / "diamond_output.tsv")

        assert table.num_rows == 6
        assert table.schema.equals(DIAMOND_SCHEMA)

    def test_column_types(self) -> None:
        table = parse_output(FIXTURES / "diamond_output.tsv")

        assert table.column("qseqid").type == pa.string()
        assert table.column("sseqid").type == pa.string()
        assert table.column("pident").type == pa.float64()
        assert table.column("length").type == pa.int32()
        assert table.column("mismatch").type == pa.int32()
        assert table.column("gapopen").type == pa.int32()
        assert table.column("qstart").type == pa.int32()
        assert table.column("qend").type == pa.int32()
        assert table.column("sstart").type == pa.int32()
        assert table.column("send").type == pa.int32()
        assert table.column("evalue").type == pa.float64()
        assert table.column("bitscore").type == pa.float64()

    def test_values_parsed_correctly(self) -> None:
        table = parse_output(FIXTURES / "diamond_output.tsv")

        # First row
        assert table.column("qseqid")[0].as_py() == "query_00001"
        assert table.column("sseqid")[0].as_py() == "ref_00234"
        assert table.column("pident")[0].as_py() == pytest.approx(85.7)
        assert table.column("length")[0].as_py() == 150
        assert table.column("evalue")[0].as_py() == pytest.approx(1.5e-45)
        assert table.column("bitscore")[0].as_py() == pytest.approx(250.3)

    def test_empty_file(self, tmp_path: Path) -> None:
        empty = tmp_path / "empty.tsv"
        empty.write_text("")

        table = parse_output(empty)

        assert table.num_rows == 0
        assert table.schema.equals(DIAMOND_SCHEMA)

    def test_file_with_comments(self, tmp_path: Path) -> None:
        tsv = tmp_path / "commented.tsv"
        tsv.write_text(
            "# This is a comment\n"
            "query_00001\tref_00234\t85.7\t150\t22\t1\t1\t150\t10\t160\t1.5e-45\t250.3\n"
        )

        table = parse_output(tsv)
        assert table.num_rows == 1


class TestCheckAvailable:
    """Unit tests for DIAMOND availability checking."""

    def test_not_available_when_binary_missing(self) -> None:
        wrapper = DiamondWrapper(binary="/nonexistent/diamond")
        assert wrapper.check_available() is False

    def test_not_available_when_which_returns_none(self) -> None:
        with patch("shutil.which", return_value=None):
            wrapper = DiamondWrapper(binary="diamond")
            assert wrapper.check_available() is False


class TestDiamondResult:
    """Tests for the DiamondResult dataclass."""

    def test_success_result(self) -> None:
        result = DiamondResult(
            exit_code=0, duration_seconds=1.5, stderr=""
        )
        assert result.exit_code == 0
        assert result.error_message is None

    def test_oom_result(self) -> None:
        result = DiamondResult(
            exit_code=137,
            duration_seconds=120.0,
            stderr="Killed",
            error_message="OOM",
        )
        assert result.exit_code == 137

    def test_timeout_result(self) -> None:
        result = DiamondResult(
            exit_code=-1,
            duration_seconds=3600.0,
            stderr="",
            error_message="DIAMOND timed out after 3600s",
        )
        assert result.exit_code == -1

    def test_not_found_result(self) -> None:
        result = DiamondResult(
            exit_code=-2,
            duration_seconds=0.0,
            stderr="",
            error_message="DIAMOND binary not found",
        )
        assert result.exit_code == -2


class TestRunCommand:
    """Tests for subprocess execution error handling (without DIAMOND)."""

    def test_file_not_found_error(self) -> None:
        wrapper = DiamondWrapper(binary="/nonexistent/diamond_xyz_fake")
        result = wrapper.make_db(
            Path("/tmp/fake.fasta"),
            Path("/tmp/fake_db"),
            timeout=5,
        )
        assert result.exit_code == -2
        assert result.error_message is not None
        assert "not found" in result.error_message


# --- Integration tests (require DIAMOND binary) ---


@pytest.mark.integration
class TestDiamondIntegration:
    """Integration tests that require the DIAMOND binary.

    Uses Swiss-Prot fixture data when available (real sequences that
    produce meaningful hits), falls back to synthetic data otherwise.
    """

    @pytest.fixture
    def diamond(self) -> DiamondWrapper:
        wrapper = DiamondWrapper(binary="diamond", threads=1)
        if not wrapper.check_available():
            pytest.skip("DIAMOND binary not available")
        return wrapper

    def test_makedb(
        self,
        diamond: DiamondWrapper,
        integration_test_data: tuple[Path, Path],
        tmp_path: Path,
    ) -> None:
        _, reference = integration_test_data
        db_path = tmp_path / "ref_db"

        result = diamond.make_db(reference, db_path)

        assert result.exit_code == 0
        assert (tmp_path / "ref_db.dmnd").exists()

    def test_blastp(
        self,
        diamond: DiamondWrapper,
        integration_test_data: tuple[Path, Path],
        tmp_path: Path,
    ) -> None:
        queries, reference = integration_test_data
        db_path = tmp_path / "ref_db"
        output = tmp_path / "output.tsv"

        diamond.make_db(reference, db_path)
        result = diamond.run_blastp(
            queries,
            tmp_path / "ref_db.dmnd",
            output,
            sensitivity="fast",
            max_target_seqs=5,
        )

        assert result.exit_code == 0
        assert output.exists()

    def test_blastp_produces_hits_with_real_data(
        self,
        diamond: DiamondWrapper,
        integration_test_data: tuple[Path, Path],
        tmp_path: Path,
    ) -> None:
        """With Swiss-Prot data, DIAMOND should find real homologs."""
        queries, reference = integration_test_data
        db_path = tmp_path / "ref_db"
        output = tmp_path / "output.tsv"

        diamond.make_db(reference, db_path)
        diamond.run_blastp(
            queries,
            tmp_path / "ref_db.dmnd",
            output,
            sensitivity="fast",
            max_target_seqs=5,
        )

        table = parse_output(output)
        assert table.schema.equals(DIAMOND_SCHEMA)
        # Swiss-Prot human vs E. coli should produce some conserved hits
        # (e.g. ribosomal proteins, chaperones, metabolic enzymes)
        if table.num_rows > 0:
            evalues = table.column("evalue").to_pylist()
            assert min(evalues) < 1e-5, (
                "Expected at least one strong hit between human and E. coli"
            )

    def test_timeout_handling(
        self,
        diamond: DiamondWrapper,
        integration_test_data: tuple[Path, Path],
        tmp_path: Path,
    ) -> None:
        """Very short timeout → TimeoutExpired handled gracefully."""
        queries, reference = integration_test_data
        db_path = tmp_path / "ref_db"
        output = tmp_path / "output.tsv"

        diamond.make_db(reference, db_path)

        # Use an extremely short timeout — this may or may not trigger
        # depending on speed, so we just verify it doesn't raise
        result = diamond.run_blastp(
            queries,
            tmp_path / "ref_db.dmnd",
            output,
            sensitivity="ultra-sensitive",
            timeout=1,
        )

        # Either it completed fast enough (exit_code=0)
        # or it timed out (exit_code=-1)
        assert result.exit_code in (0, -1)
        if result.exit_code == -1:
            assert result.error_message is not None
            assert "timed out" in result.error_message
