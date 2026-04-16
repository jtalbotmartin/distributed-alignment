"""Tests for taxonomic enrichment of merged alignment results."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from distributed_alignment.merge.merger import MERGED_SCHEMA
from distributed_alignment.taxonomy.enricher import (
    ENRICHED_SCHEMA,
    PROFILE_SCHEMA,
    compute_taxonomic_profiles,
    enrich_results,
    extract_accession,
)
from distributed_alignment.taxonomy.ncbi_loader import TaxonomyDB

FIXTURES_DIR = Path(__file__).parent / "fixtures" / "taxonomy"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_merged_parquet(path: Path, rows: list[dict[str, object]]) -> Path:
    """Write a merged-format Parquet file from row dicts."""
    arrays: dict[str, list[object]] = {f.name: [] for f in MERGED_SCHEMA}
    for row in rows:
        for name in arrays:
            arrays[name].append(row.get(name))

    table = pa.table(
        {
            name: pa.array(vals, type=field.type)
            for (name, vals), field in zip(arrays.items(), MERGED_SCHEMA, strict=True)
        },
    )
    pq.write_table(table, str(path))
    return path


def _hit(
    query_id: str = "Q001",
    subject_id: str = "sp|P0A8M3|SYT_ECOLI",
    percent_identity: float = 95.0,
    evalue: float = 1e-50,
    bitscore: float = 200.0,
    global_rank: int = 1,
) -> dict[str, object]:
    """Build a single merged-hit row with sensible defaults."""
    return {
        "query_id": query_id,
        "subject_id": subject_id,
        "percent_identity": percent_identity,
        "alignment_length": 100,
        "mismatches": 5,
        "gap_opens": 0,
        "query_start": 1,
        "query_end": 100,
        "subject_start": 1,
        "subject_end": 100,
        "evalue": evalue,
        "bitscore": bitscore,
        "global_rank": global_rank,
        "query_chunk_id": "q000",
        "ref_chunk_id": "r000",
    }


@pytest.fixture()
def taxonomy_db(tmp_path: Path) -> TaxonomyDB:
    """Build TaxonomyDB from test fixtures."""
    return TaxonomyDB.from_ncbi_dump(
        nodes_path=FIXTURES_DIR / "nodes.dmp",
        names_path=FIXTURES_DIR / "names.dmp",
        accession2taxid_path=FIXTURES_DIR / "accession2taxid.tsv",
        db_path=tmp_path / "taxonomy.duckdb",
    )


# ---------------------------------------------------------------------------
# Accession extraction
# ---------------------------------------------------------------------------


class TestExtractAccession:
    def test_swissprot_format(self) -> None:
        assert extract_accession("sp|P0A8M3|SYT_ECOLI") == "P0A8M3"

    def test_trembl_format(self) -> None:
        assert extract_accession("tr|A0A0F7XYZ|SOMETHING") == "A0A0F7XYZ"

    def test_plain_accession(self) -> None:
        assert extract_accession("P0A8M3") == "P0A8M3"

    def test_two_pipes(self) -> None:
        assert extract_accession("sp|Q12345|") == "Q12345"

    def test_single_pipe(self) -> None:
        # "db|ACC" — still split on pipe, take index 1
        assert extract_accession("db|ACC") == "ACC"


# ---------------------------------------------------------------------------
# Basic enrichment
# ---------------------------------------------------------------------------


class TestEnrichResults:
    def test_ecoli_hit_annotated(self, taxonomy_db: TaxonomyDB, tmp_path: Path) -> None:
        merged = _make_merged_parquet(
            tmp_path / "merged.parquet",
            [_hit(subject_id="sp|P0A8M3|SYT_ECOLI")],
        )
        out = enrich_results(merged, taxonomy_db, tmp_path / "enriched.parquet")
        table = pq.read_table(str(out))

        assert table.num_rows == 1
        row = table.to_pydict()
        assert row["phylum"][0] == "Pseudomonadota"
        assert row["kingdom"][0] == "Bacteria"
        assert row["species"][0] == "Escherichia coli"
        assert row["taxon_id"][0] == 83333

    def test_multiple_phyla(self, taxonomy_db: TaxonomyDB, tmp_path: Path) -> None:
        """One query with hits to E. coli, B. subtilis, and S. cerevisiae."""
        merged = _make_merged_parquet(
            tmp_path / "merged.parquet",
            [
                _hit(subject_id="sp|P0A8M3|SYT_ECOLI", global_rank=1),
                _hit(subject_id="sp|C0SP85|YUKE_BACSU", global_rank=2),
                _hit(subject_id="sp|D6VTK4|YAR1_YEAST", global_rank=3),
            ],
        )
        out = enrich_results(merged, taxonomy_db, tmp_path / "enriched.parquet")
        table = pq.read_table(str(out))

        phyla = table.column("phylum").to_pylist()
        assert "Pseudomonadota" in phyla
        assert "Bacillota" in phyla
        assert "Ascomycota" in phyla

    def test_unmapped_accession_preserved(
        self, taxonomy_db: TaxonomyDB, tmp_path: Path
    ) -> None:
        """Accession not in taxonomy DB gets 'unknown' but isn't dropped."""
        merged = _make_merged_parquet(
            tmp_path / "merged.parquet",
            [
                _hit(subject_id="sp|P0A8M3|SYT_ECOLI", global_rank=1),
                _hit(subject_id="sp|ZZZZZZ|FAKE_THING", global_rank=2),
            ],
        )
        out = enrich_results(merged, taxonomy_db, tmp_path / "enriched.parquet")
        table = pq.read_table(str(out))

        assert table.num_rows == 2

        rows = table.to_pydict()
        # Second hit should have "unknown" taxonomy
        assert rows["phylum"][1] == "unknown"
        assert rows["kingdom"][1] == "unknown"
        assert rows["species"][1] == "unknown"
        assert rows["taxon_id"][1] is None

        # First hit should be correctly annotated
        assert rows["phylum"][0] == "Pseudomonadota"

    def test_plain_accession_format(
        self, taxonomy_db: TaxonomyDB, tmp_path: Path
    ) -> None:
        """Subject ID without pipe delimiters works."""
        merged = _make_merged_parquet(
            tmp_path / "merged.parquet",
            [_hit(subject_id="P0A8M3")],
        )
        out = enrich_results(merged, taxonomy_db, tmp_path / "enriched.parquet")
        table = pq.read_table(str(out))

        row = table.to_pydict()
        assert row["phylum"][0] == "Pseudomonadota"

    def test_empty_input(self, taxonomy_db: TaxonomyDB, tmp_path: Path) -> None:
        """Zero-row input produces valid empty enriched Parquet."""
        merged = _make_merged_parquet(tmp_path / "merged.parquet", [])
        out = enrich_results(merged, taxonomy_db, tmp_path / "enriched.parquet")
        table = pq.read_table(str(out))

        assert table.num_rows == 0
        assert set(ENRICHED_SCHEMA.names).issubset(set(table.schema.names))


# ---------------------------------------------------------------------------
# Schema validation
# ---------------------------------------------------------------------------


class TestEnrichedSchema:
    def test_has_all_taxonomy_columns(
        self, taxonomy_db: TaxonomyDB, tmp_path: Path
    ) -> None:
        merged = _make_merged_parquet(
            tmp_path / "merged.parquet",
            [_hit()],
        )
        out = enrich_results(merged, taxonomy_db, tmp_path / "enriched.parquet")
        table = pq.read_table(str(out))
        col_names = set(table.schema.names)

        for expected in [
            "taxon_id",
            "species",
            "genus",
            "family",
            "order",
            "class",
            "phylum",
            "kingdom",
        ]:
            assert expected in col_names, f"Missing column: {expected}"

    def test_original_columns_preserved(
        self, taxonomy_db: TaxonomyDB, tmp_path: Path
    ) -> None:
        merged = _make_merged_parquet(
            tmp_path / "merged.parquet",
            [_hit()],
        )
        out = enrich_results(merged, taxonomy_db, tmp_path / "enriched.parquet")
        table = pq.read_table(str(out))
        col_names = set(table.schema.names)

        for original in MERGED_SCHEMA.names:
            assert original in col_names, f"Missing original column: {original}"

    def test_taxon_id_type(self, taxonomy_db: TaxonomyDB, tmp_path: Path) -> None:
        merged = _make_merged_parquet(
            tmp_path / "merged.parquet",
            [_hit()],
        )
        out = enrich_results(merged, taxonomy_db, tmp_path / "enriched.parquet")
        table = pq.read_table(str(out))
        assert table.schema.field("taxon_id").type == pa.int32()


# ---------------------------------------------------------------------------
# Taxonomic profiles
# ---------------------------------------------------------------------------


class TestTaxonomicProfiles:
    def test_profile_basic(self, taxonomy_db: TaxonomyDB, tmp_path: Path) -> None:
        """Query with hits in 3 phyla → profile has 3 rows."""
        merged = _make_merged_parquet(
            tmp_path / "merged.parquet",
            [
                _hit(
                    query_id="Q001",
                    subject_id="sp|P0A8M3|SYT_ECOLI",
                    percent_identity=95.0,
                    evalue=1e-50,
                    global_rank=1,
                ),
                _hit(
                    query_id="Q001",
                    subject_id="sp|P00350|6PGD_ECOLI",
                    percent_identity=85.0,
                    evalue=1e-40,
                    global_rank=2,
                ),
                _hit(
                    query_id="Q001",
                    subject_id="sp|C0SP85|YUKE_BACSU",
                    percent_identity=60.0,
                    evalue=1e-10,
                    global_rank=3,
                ),
                _hit(
                    query_id="Q001",
                    subject_id="sp|D6VTK4|YAR1_YEAST",
                    percent_identity=30.0,
                    evalue=1e-3,
                    global_rank=4,
                ),
            ],
        )
        enriched = tmp_path / "enriched.parquet"
        enrich_results(merged, taxonomy_db, enriched)

        profile_path = tmp_path / "profiles.parquet"
        compute_taxonomic_profiles(enriched, profile_path)
        profile = pq.read_table(str(profile_path))

        rows = profile.to_pydict()
        # Q001: 2 E. coli (Pseudomonadota), 1 B. subtilis (Bacillota),
        # 1 yeast (Ascomycota) → 3 phyla
        assert profile.num_rows == 3
        assert set(rows["phylum"]) == {
            "Pseudomonadota",
            "Bacillota",
            "Ascomycota",
        }

    def test_profile_counts(self, taxonomy_db: TaxonomyDB, tmp_path: Path) -> None:
        """Verify hit counts and statistics are correct."""
        merged = _make_merged_parquet(
            tmp_path / "merged.parquet",
            [
                _hit(
                    query_id="Q001",
                    subject_id="sp|P0A8M3|SYT_ECOLI",
                    percent_identity=90.0,
                    evalue=1e-50,
                    global_rank=1,
                ),
                _hit(
                    query_id="Q001",
                    subject_id="sp|P00350|6PGD_ECOLI",
                    percent_identity=80.0,
                    evalue=1e-40,
                    global_rank=2,
                ),
            ],
        )
        enriched = tmp_path / "enriched.parquet"
        enrich_results(merged, taxonomy_db, enriched)

        profile_path = tmp_path / "profiles.parquet"
        compute_taxonomic_profiles(enriched, profile_path)
        profile = pq.read_table(str(profile_path))

        rows = profile.to_pydict()
        # Single phylum: Pseudomonadota with 2 hits
        assert profile.num_rows == 1
        assert rows["hit_count"][0] == 2
        assert rows["mean_percent_identity"][0] == pytest.approx(85.0)
        assert rows["best_evalue"][0] == pytest.approx(1e-50)

    def test_profile_multiple_queries(
        self, taxonomy_db: TaxonomyDB, tmp_path: Path
    ) -> None:
        """Multiple queries produce separate profile entries."""
        merged = _make_merged_parquet(
            tmp_path / "merged.parquet",
            [
                _hit(query_id="Q001", subject_id="sp|P0A8M3|SYT_ECOLI"),
                _hit(query_id="Q002", subject_id="sp|C0SP85|YUKE_BACSU"),
            ],
        )
        enriched = tmp_path / "enriched.parquet"
        enrich_results(merged, taxonomy_db, enriched)

        profile_path = tmp_path / "profiles.parquet"
        compute_taxonomic_profiles(enriched, profile_path)
        profile = pq.read_table(str(profile_path))

        assert profile.num_rows == 2
        query_ids = set(profile.column("query_id").to_pylist())
        assert query_ids == {"Q001", "Q002"}

    def test_profile_schema(self, taxonomy_db: TaxonomyDB, tmp_path: Path) -> None:
        """Profile output has the expected columns and types."""
        merged = _make_merged_parquet(
            tmp_path / "merged.parquet",
            [_hit()],
        )
        enriched = tmp_path / "enriched.parquet"
        enrich_results(merged, taxonomy_db, enriched)

        profile_path = tmp_path / "profiles.parquet"
        compute_taxonomic_profiles(enriched, profile_path)
        profile = pq.read_table(str(profile_path))

        for field in PROFILE_SCHEMA:
            assert field.name in profile.schema.names, (
                f"Missing profile column: {field.name}"
            )
