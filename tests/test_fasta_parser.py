"""Tests for the streaming FASTA parser."""

from __future__ import annotations

import types
from pathlib import Path

import pytest

from distributed_alignment.ingest.fasta_parser import parse_fasta
from distributed_alignment.models import ProteinSequence

FIXTURES = Path(__file__).parent / "fixtures"


class TestValidParsing:
    """Tests for correct parsing of well-formed FASTA files."""

    def test_parse_valid_multi_sequence(self) -> None:
        sequences = list(parse_fasta(FIXTURES / "valid.fasta"))
        assert len(sequences) == 3

        assert sequences[0].id == "sp|P12345|PROT1"
        assert sequences[0].description == "sp|P12345|PROT1 Test protein 1"
        assert sequences[0].sequence == "MKWVTFISLLFLFSSAYSRGVFRRDAHKSEVAHRFK"
        assert sequences[0].length == 36

        assert sequences[1].id == "sp|P67890|PROT2"
        assert sequences[1].length == 41

        assert sequences[2].id == "sp|Q11111|PROT3"
        assert sequences[2].length == 53

    def test_multiline_sequences_concatenated(self) -> None:
        """First sequence in valid.fasta spans two lines."""
        sequences = list(parse_fasta(FIXTURES / "valid.fasta"))
        # "MKWVTFISLLFLFSSAYS" + "RGVFRRDAHKSEVAHRFK" = 36 chars
        assert sequences[0].sequence == "MKWVTFISLLFLFSSAYSRGVFRRDAHKSEVAHRFK"
        assert len(sequences[0].sequence) == 36

    def test_single_line_sequence(self) -> None:
        """Second and third sequences in valid.fasta are single-line."""
        sequences = list(parse_fasta(FIXTURES / "valid.fasta"))
        assert sequences[1].sequence == "MVLSPADKTNVKAAWGKVGAHAGEYGAEALERMFLSFPTTK"

    def test_sequences_are_uppercased(self, tmp_path: Path) -> None:
        fasta = tmp_path / "lower.fasta"
        fasta.write_text(">prot1 lowercase test\nmkwvtfis\n")
        sequences = list(parse_fasta(fasta))
        assert sequences[0].sequence == "MKWVTFIS"

    def test_yields_protein_sequence_instances(self) -> None:
        for seq in parse_fasta(FIXTURES / "valid.fasta"):
            assert isinstance(seq, ProteinSequence)

    def test_blank_lines_ignored(self, tmp_path: Path) -> None:
        fasta = tmp_path / "blanks.fasta"
        fasta.write_text(
            ">prot1 with blanks\n"
            "MKWVTFIS\n"
            "\n"
            "LLFLFSSAYS\n"
            "\n"
            ">prot2 after blank\n"
            "RGVFRRDAHK\n"
        )
        sequences = list(parse_fasta(fasta))
        assert len(sequences) == 2
        assert sequences[0].sequence == "MKWVTFISLLFLFSSAYS"
        assert sequences[1].sequence == "RGVFRRDAHK"


class TestEmptyFile:
    """Tests for empty or whitespace-only FASTA files."""

    def test_empty_file_yields_nothing(self) -> None:
        sequences = list(parse_fasta(FIXTURES / "empty.fasta"))
        assert sequences == []

    def test_whitespace_only_file(self, tmp_path: Path) -> None:
        fasta = tmp_path / "whitespace.fasta"
        fasta.write_text("   \n\n  \n")
        sequences = list(parse_fasta(fasta))
        assert sequences == []


class TestErrorHandling:
    """Tests for malformed input detection."""

    def test_malformed_no_header(self) -> None:
        with pytest.raises(ValueError, match="before any FASTA header"):
            list(parse_fasta(FIXTURES / "malformed.fasta"))

    def test_invalid_amino_acid_characters(self) -> None:
        with pytest.raises(ValueError, match="Invalid amino acid characters"):
            list(parse_fasta(FIXTURES / "invalid_chars.fasta"))

    def test_error_includes_line_number(self) -> None:
        with pytest.raises(ValueError, match=r"Line \d+"):
            list(parse_fasta(FIXTURES / "malformed.fasta"))

    def test_empty_sequence_raises(self, tmp_path: Path) -> None:
        fasta = tmp_path / "empty_seq.fasta"
        fasta.write_text(">prot1 has no sequence\n>prot2 another\nMKW\n")
        with pytest.raises(ValueError, match="no amino acid data"):
            list(parse_fasta(fasta))

    def test_empty_header_raises(self, tmp_path: Path) -> None:
        fasta = tmp_path / "empty_header.fasta"
        fasta.write_text(">\nMKWVTFIS\n")
        with pytest.raises(ValueError, match="empty FASTA header"):
            list(parse_fasta(fasta))

    def test_file_not_found(self, tmp_path: Path) -> None:
        with pytest.raises(FileNotFoundError):
            list(parse_fasta(tmp_path / "nonexistent.fasta"))

    def test_data_before_first_header(self, tmp_path: Path) -> None:
        fasta = tmp_path / "data_first.fasta"
        fasta.write_text("MKWVTFIS\n>prot1 header\nACDEFG\n")
        with pytest.raises(ValueError, match="Line 1.*before any FASTA header"):
            list(parse_fasta(fasta))


class TestMaxLengthWarning:
    """Tests for the max_length warning behaviour."""

    def test_long_sequence_warns(self, tmp_path: Path) -> None:
        fasta = tmp_path / "long.fasta"
        long_seq = "M" * 200
        fasta.write_text(f">longprot long sequence\n{long_seq}\n")
        sequences = list(parse_fasta(fasta, max_length=100))
        # Should still yield the sequence (warning only, not error)
        assert len(sequences) == 1
        assert sequences[0].length == 200

    def test_max_length_zero_disables_warning(self, tmp_path: Path) -> None:
        fasta = tmp_path / "long.fasta"
        long_seq = "M" * 200
        fasta.write_text(f">longprot long sequence\n{long_seq}\n")
        # max_length=0 disables the warning — should not raise
        sequences = list(parse_fasta(fasta, max_length=0))
        assert len(sequences) == 1


class TestGeneratorBehaviour:
    """Tests verifying the parser is a true generator."""

    def test_returns_generator(self) -> None:
        result = parse_fasta(FIXTURES / "valid.fasta")
        assert isinstance(result, types.GeneratorType)

    def test_can_iterate_one_at_a_time(self) -> None:
        gen = parse_fasta(FIXTURES / "valid.fasta")
        first = next(gen)
        assert first.id == "sp|P12345|PROT1"
        second = next(gen)
        assert second.id == "sp|P67890|PROT2"
        third = next(gen)
        assert third.id == "sp|Q11111|PROT3"
        with pytest.raises(StopIteration):
            next(gen)

    def test_large_file_is_streamable(self, tmp_path: Path) -> None:
        """Generate a 1000-sequence file and verify we can stream it."""
        fasta = tmp_path / "large.fasta"
        with fasta.open("w") as f:
            for i in range(1000):
                f.write(f">seq_{i:04d} test sequence {i}\n")
                f.write("MKWVTFISLLFLFSSAYS\n")

        gen = parse_fasta(fasta)
        # Consume one at a time — if this were a list, it wouldn't
        # be a generator
        assert isinstance(gen, types.GeneratorType)
        count = sum(1 for _ in gen)
        assert count == 1000
