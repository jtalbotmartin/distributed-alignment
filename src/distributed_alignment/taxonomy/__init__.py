"""Taxonomic enrichment via NCBI taxonomy."""

from distributed_alignment.taxonomy.enricher import (
    compute_taxonomic_profiles,
    enrich_results,
)
from distributed_alignment.taxonomy.ncbi_loader import TaxonomyDB

__all__ = ["TaxonomyDB", "compute_taxonomic_profiles", "enrich_results"]
