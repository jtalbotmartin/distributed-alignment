"""Phase 3 feature-engineering pipeline orchestrator.

Chains taxonomic enrichment → alignment features → k-mer features →
(optional) embeddings → combined feature table, registering every
output and lineage edge in the :class:`CatalogueStore`.

Starts from a merged Parquet (or directory of merged Parquets from
per-query-chunk merging).  Upstream stages (ingest, DIAMOND, merge)
live in the existing ``run`` CLI command.

The orchestration is a pure function so integration tests can call
it directly without driving through Typer.  The CLI wrapper is a
thin layer in :mod:`distributed_alignment.cli`.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path  # noqa: TCH003 — runtime use

import pyarrow.parquet as pq
import structlog

from distributed_alignment.catalogue import CatalogueStore
from distributed_alignment.features.alignment_features import (
    extract_alignment_features,
)
from distributed_alignment.features.combiner import combine_features
from distributed_alignment.features.embedding_features import load_embeddings
from distributed_alignment.features.kmer_features import extract_kmer_features
from distributed_alignment.taxonomy import TaxonomyDB
from distributed_alignment.taxonomy.enricher import enrich_results

logger = structlog.get_logger(__name__)


class RunCollisionError(RuntimeError):
    """Raised when a ``run_id`` already exists in a non-resumable state."""


@dataclass
class PipelineResult:
    """Summary of a completed pipeline run.

    Returned by :func:`run_feature_pipeline` so callers (CLI, tests)
    can inspect outputs without re-reading the catalogue.
    """

    run_id: str
    status: str  # "completed" | "failed"
    outputs: dict[str, Path] = field(default_factory=dict)
    metrics: dict[str, object] = field(default_factory=dict)
    error: str | None = None


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------


def run_feature_pipeline(
    *,
    run_id: str,
    merged_parquet_path: Path,
    chunks_dir: Path,
    features_dir: Path,
    catalogue_path: Path,
    taxonomy_db_path: Path | None = None,
    embeddings_path: Path | None = None,
    skip_enrichment: bool = False,
    skip_features: bool = False,
    feature_version: str = "v1",
    git_commit: str | None = None,
    config: dict[str, object] | None = None,
    force_rerun: bool = False,
) -> PipelineResult:
    """Run the Phase 3 feature pipeline.

    Args:
        run_id: Unique identifier for this run. Used in output paths
            and catalogue records.
        merged_parquet_path: Path to merged alignment Parquet
            (a single file or a directory of per-chunk files).
        chunks_dir: Pipeline work directory containing
            ``chunks/queries/*.parquet`` — needed for k-mer features
            and zero-hit query inclusion.
        features_dir: Base directory for feature outputs. Actual
            outputs land under ``features_dir / run_id /``.
        catalogue_path: Path to (or for) the catalogue DuckDB file.
        taxonomy_db_path: Optional NCBI taxonomy DuckDB. If provided
            and ``skip_enrichment`` is False, runs the enrichment
            stage.
        embeddings_path: Optional pre-computed ESM-2 embeddings
            Parquet. If provided, embeddings are joined into the
            combined feature table.
        skip_enrichment: If True, skip the enrichment stage even if
            ``taxonomy_db_path`` is set. Feature stages then read
            from the merged Parquet directly.
        skip_features: If True, stop after the enrichment stage (or
            after merge if enrichment also skipped). No feature
            Parquets are written.
        feature_version: Propagated into feature output metadata.
        git_commit: Optional git SHA recorded with the run.
        config: Arbitrary config dict stored with the run (typically
            CLI args).
        force_rerun: If True, overwrite an existing non-failed run
            with the same ``run_id``. Default False raises
            :class:`RunCollisionError` on collision.

    Returns:
        :class:`PipelineResult` with output paths and metrics.
    """
    log = logger.bind(run_id=run_id)
    out_dir = features_dir / run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    outputs: dict[str, Path] = {}
    stages_completed: list[str] = []
    stages_skipped: list[str] = []
    durations: dict[str, float] = {}
    row_counts: dict[str, int] = {}

    with CatalogueStore(catalogue_path) as cat:
        # -- Run-ID collision guard ----------------------------------
        existing = cat.get_run(run_id)
        if existing is not None and not force_rerun:
            status = existing["status"]
            if status != "failed":
                msg = (
                    f"Run {run_id!r} already exists with status "
                    f"{status!r}. Pass force_rerun=True (CLI: "
                    f"--force-rerun) to overwrite."
                )
                raise RunCollisionError(msg)

        cat.register_run(
            run_id,
            datetime.now(tz=UTC),
            config=config,
            git_commit=git_commit,
        )
        log.info("pipeline_started")

        try:
            # -- Register the merged input --------------------------
            merged_ds_id = f"merged_{run_id}"
            cat.register_dataset(
                merged_ds_id,
                "merged",
                merged_parquet_path,
                schema_version=feature_version,
                created_by_run=run_id,
                num_rows=_count_rows(merged_parquet_path),
            )
            outputs["merged"] = merged_parquet_path

            # -- Stage A: enrichment --------------------------------
            features_input_path = merged_parquet_path
            features_parent_ds = merged_ds_id

            if taxonomy_db_path is not None and not skip_enrichment:
                t0 = time.monotonic()
                enriched_path = out_dir / "enriched.parquet"
                log.info("stage_start", stage="enrichment")

                tax = TaxonomyDB(taxonomy_db_path)
                try:
                    enrich_results(merged_parquet_path, tax, enriched_path)
                finally:
                    tax.close()

                enriched_ds_id = f"enriched_{run_id}"
                cat.register_dataset(
                    enriched_ds_id,
                    "enriched",
                    enriched_path,
                    schema_version=feature_version,
                    created_by_run=run_id,
                    num_rows=_count_rows(enriched_path),
                )
                cat.register_lineage(enriched_ds_id, merged_ds_id, "enriched_from")
                outputs["enriched"] = enriched_path
                features_input_path = enriched_path
                features_parent_ds = enriched_ds_id

                durations["enrichment"] = time.monotonic() - t0
                row_counts["enriched"] = _count_rows(enriched_path)
                stages_completed.append("enrichment")
                log.info(
                    "stage_completed",
                    stage="enrichment",
                    duration_seconds=durations["enrichment"],
                    num_rows=row_counts["enriched"],
                )
            else:
                stages_skipped.append("enrichment")

            if skip_features:
                stages_skipped.extend(
                    [
                        "alignment_features",
                        "kmer_features",
                        "combine",
                    ]
                )
                log.info("stages_skipped", skipped=stages_skipped)
            else:
                # -- Stage B: alignment features ------------------
                t0 = time.monotonic()
                alignment_path = out_dir / "alignment_features.parquet"
                log.info("stage_start", stage="alignment_features")
                extract_alignment_features(
                    features_input_path,
                    chunks_dir,
                    alignment_path,
                    run_id=run_id,
                    feature_version=feature_version,
                )
                alignment_ds_id = f"alignment_features_{run_id}"
                cat.register_dataset(
                    alignment_ds_id,
                    "features",
                    alignment_path,
                    schema_version=feature_version,
                    created_by_run=run_id,
                    num_rows=_count_rows(alignment_path),
                )
                cat.register_lineage(
                    alignment_ds_id,
                    features_parent_ds,
                    "features_from",
                )
                outputs["alignment_features"] = alignment_path
                durations["alignment_features"] = time.monotonic() - t0
                row_counts["alignment_features"] = _count_rows(alignment_path)
                stages_completed.append("alignment_features")
                log.info(
                    "stage_completed",
                    stage="alignment_features",
                    duration_seconds=durations["alignment_features"],
                    num_rows=row_counts["alignment_features"],
                )

                # -- Stage C: k-mer features ----------------------
                t0 = time.monotonic()
                kmer_path = out_dir / "kmer_features.parquet"
                log.info("stage_start", stage="kmer_features")
                extract_kmer_features(
                    chunks_dir,
                    kmer_path,
                    run_id=run_id,
                    feature_version=feature_version,
                )
                kmer_ds_id = f"kmer_features_{run_id}"
                chunks_queries_ds_id = f"chunks_queries_{run_id}"
                cat.register_dataset(
                    chunks_queries_ds_id,
                    "chunk",
                    chunks_dir / "queries",
                    created_by_run=run_id,
                )
                cat.register_dataset(
                    kmer_ds_id,
                    "features",
                    kmer_path,
                    schema_version=feature_version,
                    created_by_run=run_id,
                    num_rows=_count_rows(kmer_path),
                )
                cat.register_lineage(
                    kmer_ds_id,
                    chunks_queries_ds_id,
                    "features_from",
                )
                outputs["kmer_features"] = kmer_path
                durations["kmer_features"] = time.monotonic() - t0
                row_counts["kmer_features"] = _count_rows(kmer_path)
                stages_completed.append("kmer_features")
                log.info(
                    "stage_completed",
                    stage="kmer_features",
                    duration_seconds=durations["kmer_features"],
                    num_rows=row_counts["kmer_features"],
                )

                # -- Stage D: embeddings (register only) ----------
                embeddings_ds_id: str | None = None
                if embeddings_path is not None:
                    # Validate on load so a malformed file fails
                    # before combine — propagates ValueError.
                    emb_table = load_embeddings(embeddings_path)
                    embeddings_ds_id = f"embeddings_{run_id}"
                    cat.register_dataset(
                        embeddings_ds_id,
                        "embeddings",
                        embeddings_path,
                        schema_version=feature_version,
                        # Pre-computed outside any pipeline run.
                        created_by_run=None,
                        num_rows=emb_table.num_rows,
                    )
                    row_counts["embeddings"] = emb_table.num_rows
                    stages_completed.append("embeddings")
                else:
                    stages_skipped.append("embeddings")

                # -- Stage E: combine -----------------------------
                t0 = time.monotonic()
                combined_path = out_dir / "combined_features.parquet"
                log.info("stage_start", stage="combine")
                combine_features(
                    alignment_features_path=alignment_path,
                    kmer_features_path=kmer_path,
                    output_path=combined_path,
                    run_id=run_id,
                    embeddings_path=embeddings_path,
                    feature_version=feature_version,
                )
                combined_ds_id = f"combined_features_{run_id}"
                cat.register_dataset(
                    combined_ds_id,
                    "features",
                    combined_path,
                    schema_version=feature_version,
                    created_by_run=run_id,
                    num_rows=_count_rows(combined_path),
                )
                cat.register_lineage(
                    combined_ds_id,
                    alignment_ds_id,
                    "combined_from",
                )
                cat.register_lineage(combined_ds_id, kmer_ds_id, "combined_from")
                if embeddings_ds_id is not None:
                    cat.register_lineage(
                        combined_ds_id,
                        embeddings_ds_id,
                        "combined_from",
                    )
                outputs["combined_features"] = combined_path
                durations["combine"] = time.monotonic() - t0
                row_counts["combined_features"] = _count_rows(combined_path)
                stages_completed.append("combine")
                log.info(
                    "stage_completed",
                    stage="combine",
                    duration_seconds=durations["combine"],
                    num_rows=row_counts["combined_features"],
                )

            # -- Success --------------------------------------------
            metrics: dict[str, object] = {
                "stages_completed": stages_completed,
                "stages_skipped": stages_skipped,
                "durations_seconds": durations,
                "row_counts": row_counts,
            }
            cat.complete_run(run_id, datetime.now(tz=UTC), metrics=metrics)
            log.info(
                "pipeline_completed",
                stages_completed=stages_completed,
                stages_skipped=stages_skipped,
            )
            return PipelineResult(
                run_id=run_id,
                status="completed",
                outputs=outputs,
                metrics=metrics,
            )

        except Exception as exc:
            failed_stage = stages_completed[-1] if stages_completed else "<pre-stage>"
            error_metrics: dict[str, object] = {
                "stages_completed": stages_completed,
                "stages_skipped": stages_skipped,
                "durations_seconds": durations,
                "row_counts": row_counts,
                "error": str(exc),
                "failed_after": failed_stage,
            }
            cat.fail_run(
                run_id,
                datetime.now(tz=UTC),
                metrics=error_metrics,
            )
            log.error(
                "pipeline_failed",
                failed_after=failed_stage,
                error=str(exc),
            )
            raise


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _count_rows(path: Path) -> int:
    """Count rows in a Parquet file or directory of Parquet files."""
    if path.is_file():
        return pq.read_table(str(path)).num_rows
    if path.is_dir():
        files = sorted(path.glob("*.parquet"))
        return sum(pq.read_table(str(f)).num_rows for f in files)
    return 0
