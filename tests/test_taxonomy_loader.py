"""Tests for the NCBI taxonomy loader."""

from __future__ import annotations

import time
from pathlib import Path

import pytest

from distributed_alignment.taxonomy.ncbi_loader import (
    TaxonomyDB,
    _parse_accession2taxid,
    _parse_names,
    _parse_nodes,
)

FIXTURES_DIR = Path(__file__).parent / "fixtures" / "taxonomy"


# ---------------------------------------------------------------------------
# Parsing tests
# ---------------------------------------------------------------------------


class TestParseNodes:
    def test_parses_root(self) -> None:
        rows = _parse_nodes(FIXTURES_DIR / "nodes.dmp")
        by_id = {r[0]: r for r in rows}
        # Root points to itself
        assert by_id[1] == (1, 1, "no rank")

    def test_ecoli_parent(self) -> None:
        rows = _parse_nodes(FIXTURES_DIR / "nodes.dmp")
        by_id = {r[0]: r for r in rows}
        # E. coli (562) parent is Escherichia (561), rank species
        assert by_id[562] == (562, 561, "species")

    def test_bacteria_is_superkingdom(self) -> None:
        rows = _parse_nodes(FIXTURES_DIR / "nodes.dmp")
        by_id = {r[0]: r for r in rows}
        assert by_id[2][2] == "superkingdom"

    def test_row_count(self) -> None:
        rows = _parse_nodes(FIXTURES_DIR / "nodes.dmp")
        # Sanity: fixture has a meaningful number of nodes
        assert len(rows) > 100


class TestParseNames:
    def test_filters_scientific_only(self) -> None:
        rows = _parse_names(FIXTURES_DIR / "names.dmp")
        # E. coli has scientific + common + synonym entries in fixture;
        # only scientific should come through
        ecoli_rows = [r for r in rows if r[0] == 562]
        assert len(ecoli_rows) == 1
        assert ecoli_rows[0][1] == "Escherichia coli"

    def test_bacteria_name(self) -> None:
        rows = _parse_names(FIXTURES_DIR / "names.dmp")
        by_id = {r[0]: r[1] for r in rows}
        assert by_id[2] == "Bacteria"

    def test_synonyms_excluded(self) -> None:
        """Synonyms like 'Proteobacteria' for Pseudomonadota should be excluded."""
        rows = _parse_names(FIXTURES_DIR / "names.dmp")
        names = [r[1] for r in rows]
        assert "Proteobacteria" not in names
        # But the scientific name is present
        by_id = {r[0]: r[1] for r in rows}
        assert by_id[1224] == "Pseudomonadota"


class TestParseAccession2Taxid:
    def test_maps_accession_to_taxon(self) -> None:
        rows = _parse_accession2taxid(FIXTURES_DIR / "accession2taxid.tsv")
        by_acc = {r[0]: r[1] for r in rows}
        assert by_acc["P0A8M3"] == 83333

    def test_skips_header(self) -> None:
        rows = _parse_accession2taxid(FIXTURES_DIR / "accession2taxid.tsv")
        accessions = [r[0] for r in rows]
        assert "accession" not in accessions

    def test_multiple_organisms(self) -> None:
        rows = _parse_accession2taxid(FIXTURES_DIR / "accession2taxid.tsv")
        by_acc = {r[0]: r[1] for r in rows}
        assert by_acc["C0SP85"] == 224308  # B. subtilis
        assert by_acc["A0A087X1C5"] == 9606  # H. sapiens
        assert by_acc["Q182W3"] == 272563  # C. difficile


# ---------------------------------------------------------------------------
# TaxonomyDB construction
# ---------------------------------------------------------------------------


@pytest.fixture()
def taxonomy_db(tmp_path: Path) -> TaxonomyDB:
    """Build a TaxonomyDB from the test fixtures."""
    db_path = tmp_path / "taxonomy.duckdb"
    return TaxonomyDB.from_ncbi_dump(
        nodes_path=FIXTURES_DIR / "nodes.dmp",
        names_path=FIXTURES_DIR / "names.dmp",
        accession2taxid_path=FIXTURES_DIR / "accession2taxid.tsv",
        db_path=db_path,
    )


class TestTaxonomyDBConstruction:
    def test_creates_db_file(self, tmp_path: Path) -> None:
        db_path = tmp_path / "taxonomy.duckdb"
        assert not db_path.exists()
        TaxonomyDB.from_ncbi_dump(
            FIXTURES_DIR / "nodes.dmp",
            FIXTURES_DIR / "names.dmp",
            FIXTURES_DIR / "accession2taxid.tsv",
            db_path,
        )
        assert db_path.exists()

    def test_cache_reuse(self, tmp_path: Path) -> None:
        """Second call to from_ncbi_dump should reuse the cached DB."""
        db_path = tmp_path / "taxonomy.duckdb"

        t0 = time.monotonic()
        db1 = TaxonomyDB.from_ncbi_dump(
            FIXTURES_DIR / "nodes.dmp",
            FIXTURES_DIR / "names.dmp",
            FIXTURES_DIR / "accession2taxid.tsv",
            db_path,
        )
        t_build = time.monotonic() - t0

        t0 = time.monotonic()
        db2 = TaxonomyDB.from_ncbi_dump(
            FIXTURES_DIR / "nodes.dmp",
            FIXTURES_DIR / "names.dmp",
            FIXTURES_DIR / "accession2taxid.tsv",
            db_path,
        )
        t_cache = time.monotonic() - t0

        # Cached load should produce the same results
        lineage1 = db1.get_lineage(562)
        lineage2 = db2.get_lineage(562)
        assert lineage1 == lineage2

        # Cached should be faster (or at least not do a full rebuild)
        # This is a loose check — on fast disks both are fast
        assert t_cache <= t_build + 0.5

    def test_missing_db_raises(self, tmp_path: Path) -> None:
        with pytest.raises(FileNotFoundError):
            TaxonomyDB(tmp_path / "nonexistent.duckdb")


# ---------------------------------------------------------------------------
# Lineage lookups
# ---------------------------------------------------------------------------


class TestLineageEcoli:
    """Full lineage check for E. coli (taxon 562)."""

    def test_species(self, taxonomy_db: TaxonomyDB) -> None:
        lineage = taxonomy_db.get_lineage(562)
        assert lineage["species"] == "Escherichia coli"

    def test_genus(self, taxonomy_db: TaxonomyDB) -> None:
        lineage = taxonomy_db.get_lineage(562)
        assert lineage["genus"] == "Escherichia"

    def test_family(self, taxonomy_db: TaxonomyDB) -> None:
        lineage = taxonomy_db.get_lineage(562)
        assert lineage["family"] == "Enterobacteriaceae"

    def test_order(self, taxonomy_db: TaxonomyDB) -> None:
        lineage = taxonomy_db.get_lineage(562)
        assert lineage["order"] == "Enterobacterales"

    def test_class(self, taxonomy_db: TaxonomyDB) -> None:
        lineage = taxonomy_db.get_lineage(562)
        assert lineage["class"] == "Gammaproteobacteria"

    def test_phylum(self, taxonomy_db: TaxonomyDB) -> None:
        lineage = taxonomy_db.get_lineage(562)
        assert lineage["phylum"] == "Pseudomonadota"

    def test_kingdom(self, taxonomy_db: TaxonomyDB) -> None:
        lineage = taxonomy_db.get_lineage(562)
        assert lineage["kingdom"] == "Bacteria"

    def test_from_strain(self, taxonomy_db: TaxonomyDB) -> None:
        """Looking up from strain ID (83333) gives the same lineage."""
        lineage = taxonomy_db.get_lineage(83333)
        assert lineage["species"] == "Escherichia coli"
        assert lineage["kingdom"] == "Bacteria"


class TestLineageHomoSapiens:
    """Full lineage check for Homo sapiens (taxon 9606)."""

    def test_species(self, taxonomy_db: TaxonomyDB) -> None:
        lineage = taxonomy_db.get_lineage(9606)
        assert lineage["species"] == "Homo sapiens"

    def test_genus(self, taxonomy_db: TaxonomyDB) -> None:
        lineage = taxonomy_db.get_lineage(9606)
        assert lineage["genus"] == "Homo"

    def test_family(self, taxonomy_db: TaxonomyDB) -> None:
        lineage = taxonomy_db.get_lineage(9606)
        assert lineage["family"] == "Hominidae"

    def test_order(self, taxonomy_db: TaxonomyDB) -> None:
        lineage = taxonomy_db.get_lineage(9606)
        assert lineage["order"] == "Primates"

    def test_class(self, taxonomy_db: TaxonomyDB) -> None:
        lineage = taxonomy_db.get_lineage(9606)
        assert lineage["class"] == "Mammalia"

    def test_phylum(self, taxonomy_db: TaxonomyDB) -> None:
        lineage = taxonomy_db.get_lineage(9606)
        assert lineage["phylum"] == "Chordata"

    def test_kingdom(self, taxonomy_db: TaxonomyDB) -> None:
        lineage = taxonomy_db.get_lineage(9606)
        assert lineage["kingdom"] == "Eukaryota"


class TestLineageOtherOrganisms:
    """Phylum-level checks for other test organisms."""

    def test_bacillus_subtilis_in_bacillota(self, taxonomy_db: TaxonomyDB) -> None:
        # 224308 = B. subtilis 168 strain, 1423 = species
        lineage = taxonomy_db.get_lineage(224308)
        assert lineage["phylum"] == "Bacillota"
        assert lineage["kingdom"] == "Bacteria"
        assert lineage["species"] == "Bacillus subtilis"

    def test_yeast_in_ascomycota(self, taxonomy_db: TaxonomyDB) -> None:
        # 559292 = S. cerevisiae S288C, 4932 = species
        lineage = taxonomy_db.get_lineage(559292)
        assert lineage["phylum"] == "Ascomycota"
        assert lineage["kingdom"] == "Eukaryota"
        assert lineage["species"] == "Saccharomyces cerevisiae"

    def test_methanosarcina_in_archaea(self, taxonomy_db: TaxonomyDB) -> None:
        # 188937 = M. acetivorans C2A
        lineage = taxonomy_db.get_lineage(188937)
        assert lineage["phylum"] == "Euryarchaeota"
        assert lineage["kingdom"] == "Archaea"

    def test_sulfolobus_in_thermoproteota(self, taxonomy_db: TaxonomyDB) -> None:
        lineage = taxonomy_db.get_lineage(273057)
        assert lineage["phylum"] == "Thermoproteota"
        assert lineage["kingdom"] == "Archaea"

    def test_campylobacter_in_campylobacterota(self, taxonomy_db: TaxonomyDB) -> None:
        lineage = taxonomy_db.get_lineage(192222)
        assert lineage["phylum"] == "Campylobacterota"
        assert lineage["kingdom"] == "Bacteria"

    def test_arabidopsis_in_streptophyta(self, taxonomy_db: TaxonomyDB) -> None:
        lineage = taxonomy_db.get_lineage(3702)
        assert lineage["phylum"] == "Streptophyta"
        assert lineage["kingdom"] == "Eukaryota"


class TestLineagePathogens:
    """Lineage checks for clinically relevant organisms."""

    def test_c_difficile(self, taxonomy_db: TaxonomyDB) -> None:
        lineage = taxonomy_db.get_lineage(272563)
        assert lineage["species"] == "Clostridioides difficile"
        assert lineage["phylum"] == "Bacillota"

    def test_salmonella(self, taxonomy_db: TaxonomyDB) -> None:
        lineage = taxonomy_db.get_lineage(99287)
        assert lineage["genus"] == "Salmonella"
        assert lineage["phylum"] == "Pseudomonadota"

    def test_s_aureus(self, taxonomy_db: TaxonomyDB) -> None:
        lineage = taxonomy_db.get_lineage(93061)
        assert lineage["species"] == "Staphylococcus aureus"
        assert lineage["phylum"] == "Bacillota"

    def test_k_pneumoniae(self, taxonomy_db: TaxonomyDB) -> None:
        lineage = taxonomy_db.get_lineage(272620)
        assert lineage["species"] == "Klebsiella pneumoniae"
        assert lineage["phylum"] == "Pseudomonadota"


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    def test_unknown_taxon_returns_none_values(self, taxonomy_db: TaxonomyDB) -> None:
        lineage = taxonomy_db.get_lineage(999999999)
        assert all(v is None for v in lineage.values())
        # Should still have the expected keys
        assert "species" in lineage
        assert "kingdom" in lineage

    def test_unknown_accession_returns_none(self, taxonomy_db: TaxonomyDB) -> None:
        assert taxonomy_db.get_taxon_id_for_accession("NONEXISTENT") is None

    def test_lineage_for_unknown_accession(self, taxonomy_db: TaxonomyDB) -> None:
        lineage = taxonomy_db.get_lineage_for_accession("NONEXISTENT")
        assert all(v is None for v in lineage.values())


# ---------------------------------------------------------------------------
# Accession lookups
# ---------------------------------------------------------------------------


class TestAccessionLookup:
    def test_ecoli_accession(self, taxonomy_db: TaxonomyDB) -> None:
        assert taxonomy_db.get_taxon_id_for_accession("P0A8M3") == 83333

    def test_human_accession(self, taxonomy_db: TaxonomyDB) -> None:
        assert taxonomy_db.get_taxon_id_for_accession("A0A087X1C5") == 9606

    def test_lineage_for_accession(self, taxonomy_db: TaxonomyDB) -> None:
        lineage = taxonomy_db.get_lineage_for_accession("P0A8M3")
        assert lineage["species"] == "Escherichia coli"
        assert lineage["kingdom"] == "Bacteria"


# ---------------------------------------------------------------------------
# Batch lookups
# ---------------------------------------------------------------------------


class TestBatchLineage:
    def test_batch_returns_all_ids(self, taxonomy_db: TaxonomyDB) -> None:
        ids = [562, 9606, 224308, 559292, 188937]
        result = taxonomy_db.batch_lineage(ids)
        assert set(result.keys()) == set(ids)

    def test_batch_correct_lineages(self, taxonomy_db: TaxonomyDB) -> None:
        ids = [562, 9606, 188937]
        result = taxonomy_db.batch_lineage(ids)
        assert result[562]["kingdom"] == "Bacteria"
        assert result[9606]["kingdom"] == "Eukaryota"
        assert result[188937]["kingdom"] == "Archaea"

    def test_batch_with_unknown(self, taxonomy_db: TaxonomyDB) -> None:
        ids = [562, 999999999]
        result = taxonomy_db.batch_lineage(ids)
        assert result[562]["species"] == "Escherichia coli"
        assert all(v is None for v in result[999999999].values())


# ---------------------------------------------------------------------------
# Ground truth cross-check
# ---------------------------------------------------------------------------


class TestGroundTruthCrossCheck:
    """Verify lineage lookups against ground_truth.json phyla."""

    def test_ground_truth_phyla(self, taxonomy_db: TaxonomyDB) -> None:
        """Spot-check that taxon IDs from ground_truth.json resolve
        to the expected phyla."""
        import json

        gt_path = Path(__file__).parent / "fixtures" / "ground_truth.json"
        if not gt_path.exists():
            pytest.skip("ground_truth.json not available")

        ground_truth = json.loads(gt_path.read_text())

        # Sample a few accessions per phylum
        checked = 0
        for accession, info in list(ground_truth.items())[:20]:
            taxon_id = int(info["taxon_id"])
            expected_phylum = info["phylum"]

            lineage = taxonomy_db.get_lineage(taxon_id)
            if lineage["phylum"] is not None:
                assert lineage["phylum"] == expected_phylum, (
                    f"Accession {accession} (taxon {taxon_id}): "
                    f"expected phylum {expected_phylum}, "
                    f"got {lineage['phylum']}"
                )
                checked += 1

        assert checked > 0, "No ground truth entries could be verified"
