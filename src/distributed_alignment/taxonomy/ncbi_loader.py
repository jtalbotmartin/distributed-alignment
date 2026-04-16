"""NCBI taxonomy loader backed by DuckDB.

Parses NCBI taxonomy dump files (nodes.dmp, names.dmp) and protein
accession-to-taxon mappings into a cached DuckDB database, then
provides lineage lookups by walking the taxonomy tree.
"""

from __future__ import annotations

from pathlib import Path  # noqa: TCH003 — used at runtime in function bodies

import duckdb
import structlog

logger = structlog.get_logger(__name__)

# NCBI ranks we extract, mapped to output dict keys.
_RANK_TO_KEY: dict[str, str] = {
    "species": "species",
    "genus": "genus",
    "family": "family",
    "order": "order",
    "class": "class",
    "phylum": "phylum",
    "superkingdom": "kingdom",
}

_LINEAGE_KEYS: list[str] = list(dict.fromkeys(_RANK_TO_KEY.values()))


def _empty_lineage() -> dict[str, str | None]:
    """Return a lineage dict with all keys set to None."""
    return {k: None for k in _LINEAGE_KEYS}


# ---------------------------------------------------------------------------
# DMP / TSV parsers
# ---------------------------------------------------------------------------


def _parse_nodes(path: Path) -> list[tuple[int, int, str]]:
    """Parse NCBI nodes.dmp into (taxon_id, parent_taxon_id, rank) tuples."""
    rows: list[tuple[int, int, str]] = []
    with path.open() as fh:
        for line in fh:
            # Strip trailing \t| that terminates each NCBI dmp line
            line = line.strip().rstrip("|")
            parts = line.split("\t|\t")
            if len(parts) >= 3:
                taxon_id = int(parts[0].strip())
                parent_id = int(parts[1].strip())
                rank = parts[2].strip()
                rows.append((taxon_id, parent_id, rank))
    return rows


def _parse_names(path: Path) -> list[tuple[int, str]]:
    """Parse NCBI names.dmp, keeping only scientific names."""
    rows: list[tuple[int, str]] = []
    with path.open() as fh:
        for line in fh:
            parts = line.split("\t|\t")
            if len(parts) >= 4:
                name_class = parts[3].strip().rstrip("|").strip()
                if name_class == "scientific name":
                    taxon_id = int(parts[0].strip())
                    name = parts[1].strip()
                    rows.append((taxon_id, name))
    return rows


def _parse_accession2taxid(path: Path) -> list[tuple[str, int]]:
    """Parse tab-separated accession-to-taxid file (NCBI format).

    Expects columns: accession, accession.version, taxid, gi.
    Skips the header line.
    """
    rows: list[tuple[str, int]] = []
    with path.open() as fh:
        header = next(fh, None)  # skip header
        if header is None:
            return rows
        for line in fh:
            parts = line.strip().split("\t")
            if len(parts) >= 3:
                accession = parts[0]
                taxon_id = int(parts[2])
                rows.append((accession, taxon_id))
    return rows


# ---------------------------------------------------------------------------
# TaxonomyDB
# ---------------------------------------------------------------------------


class TaxonomyDB:
    """Queryable taxonomy database backed by DuckDB.

    The taxonomy tree (nodes + names) is loaded into Python dicts for
    fast lineage walking.  Accession-to-taxon lookups use the DuckDB
    index for efficiency on large mappings.
    """

    def __init__(self, db_path: Path) -> None:
        """Open an existing taxonomy DuckDB database.

        Args:
            db_path: Path to a DuckDB file previously created by
                :meth:`from_ncbi_dump`.

        Raises:
            FileNotFoundError: If *db_path* does not exist.
        """
        if not db_path.exists():
            msg = f"Taxonomy database not found: {db_path}"
            raise FileNotFoundError(msg)

        self._db_path = db_path
        self._conn: duckdb.DuckDBPyConnection = duckdb.connect(
            str(db_path), read_only=True
        )

        self._parents: dict[int, int] = {}
        self._ranks: dict[int, str] = {}
        self._names: dict[int, str] = {}
        self._load_tree()

        logger.info(
            "taxonomy_db_loaded",
            db_path=str(db_path),
            node_count=len(self._parents),
        )

    # -- construction -------------------------------------------------------

    @classmethod
    def from_ncbi_dump(
        cls,
        nodes_path: Path,
        names_path: Path,
        accession2taxid_path: Path,
        db_path: Path,
    ) -> TaxonomyDB:
        """Parse NCBI dump files and create a cached DuckDB database.

        If *db_path* already exists, the dump files are **not** re-parsed;
        the cached database is loaded directly.

        Args:
            nodes_path: Path to ``nodes.dmp``.
            names_path: Path to ``names.dmp``.
            accession2taxid_path: Path to the accession-to-taxid TSV
                (e.g. ``prot.accession2taxid`` or a small fixture subset).
            db_path: Where to write (or read) the DuckDB file.

        Returns:
            A ready-to-query :class:`TaxonomyDB` instance.
        """
        if db_path.exists():
            logger.info("taxonomy_cache_hit", db_path=str(db_path))
            return cls(db_path)

        logger.info(
            "taxonomy_building_db",
            nodes=str(nodes_path),
            names=str(names_path),
            accessions=str(accession2taxid_path),
            db_path=str(db_path),
        )

        db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = duckdb.connect(str(db_path))
        try:
            conn.execute(
                """
                CREATE TABLE nodes (
                    taxon_id INTEGER PRIMARY KEY,
                    parent_taxon_id INTEGER NOT NULL,
                    rank VARCHAR NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE names (
                    taxon_id INTEGER PRIMARY KEY,
                    scientific_name VARCHAR NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE accession2taxid (
                    accession VARCHAR PRIMARY KEY,
                    taxon_id INTEGER NOT NULL
                )
                """
            )

            nodes_data = _parse_nodes(nodes_path)
            conn.executemany("INSERT INTO nodes VALUES (?, ?, ?)", nodes_data)
            logger.info("taxonomy_nodes_loaded", count=len(nodes_data))

            names_data = _parse_names(names_path)
            conn.executemany("INSERT INTO names VALUES (?, ?)", names_data)
            logger.info("taxonomy_names_loaded", count=len(names_data))

            acc_data = _parse_accession2taxid(accession2taxid_path)
            conn.executemany("INSERT INTO accession2taxid VALUES (?, ?)", acc_data)
            logger.info("taxonomy_accessions_loaded", count=len(acc_data))
        finally:
            conn.close()

        return cls(db_path)

    # -- internal -----------------------------------------------------------

    def _load_tree(self) -> None:
        """Load node/name tables into Python dicts for fast lineage walks."""
        rows = self._conn.execute(
            "SELECT taxon_id, parent_taxon_id, rank FROM nodes"
        ).fetchall()
        for tid, pid, rank in rows:
            self._parents[tid] = pid
            self._ranks[tid] = rank

        rows = self._conn.execute(
            "SELECT taxon_id, scientific_name FROM names"
        ).fetchall()
        for tid, name in rows:
            self._names[tid] = name

    # -- queries ------------------------------------------------------------

    def get_lineage(self, taxon_id: int) -> dict[str, str | None]:
        """Walk the taxonomy tree upward and collect ranked ancestors.

        Args:
            taxon_id: NCBI taxon ID to start from.

        Returns:
            Dict with keys ``species``, ``genus``, ``family``, ``order``,
            ``class``, ``phylum``, ``kingdom`` (mapped from NCBI
            *superkingdom*).  Values are scientific names or ``None`` if
            the rank is absent from the lineage path.
        """
        result = _empty_lineage()

        current = taxon_id
        visited: set[int] = set()

        while current in self._parents and current not in visited:
            visited.add(current)

            rank = self._ranks.get(current, "")
            if rank in _RANK_TO_KEY:
                key = _RANK_TO_KEY[rank]
                if result[key] is None:
                    result[key] = self._names.get(current)

            parent = self._parents[current]
            if parent == current:
                break
            current = parent

        return result

    def get_taxon_id_for_accession(self, accession: str) -> int | None:
        """Map a protein accession to its NCBI taxon ID.

        Args:
            accession: UniProt / Swiss-Prot accession (e.g. ``"P0A8M3"``).

        Returns:
            Taxon ID, or ``None`` if the accession is not in the database.
        """
        row = self._conn.execute(
            "SELECT taxon_id FROM accession2taxid WHERE accession = ?",
            [accession],
        ).fetchone()
        return int(row[0]) if row else None

    def get_lineage_for_accession(self, accession: str) -> dict[str, str | None]:
        """Convenience: accession -> taxon_id -> lineage in one call.

        Returns an all-``None`` lineage if the accession is unknown.
        """
        taxon_id = self.get_taxon_id_for_accession(accession)
        if taxon_id is None:
            return _empty_lineage()
        return self.get_lineage(taxon_id)

    def batch_lineage(self, taxon_ids: list[int]) -> dict[int, dict[str, str | None]]:
        """Look up lineages for multiple taxon IDs.

        Args:
            taxon_ids: List of NCBI taxon IDs.

        Returns:
            Mapping of taxon ID to lineage dict.
        """
        return {tid: self.get_lineage(tid) for tid in taxon_ids}

    def close(self) -> None:
        """Close the underlying DuckDB connection."""
        self._conn.close()
