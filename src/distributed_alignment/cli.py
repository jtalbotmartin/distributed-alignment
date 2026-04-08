"""CLI entry point for distributed-alignment.

Uses Typer for command-line interface with subcommands for each pipeline stage.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated

import typer

app = typer.Typer(
    name="distributed-alignment",
    help="Distributed, fault-tolerant protein sequence alignment pipeline.",
    no_args_is_help=True,
)


def _generate_run_id() -> str:
    """Generate a timestamped run ID."""
    return f"run_{datetime.now(tz=UTC).strftime('%Y%m%d_%H%M%S')}"


@app.command()
def ingest(
    queries: Annotated[
        Path,
        typer.Option(help="Path to query FASTA file", exists=True),
    ],
    reference: Annotated[
        Path,
        typer.Option(help="Path to reference FASTA file", exists=True),
    ],
    output_dir: Annotated[
        Path,
        typer.Option(help="Output directory for chunks and manifests"),
    ] = Path("./work"),
    chunk_size: Annotated[
        int,
        typer.Option(help="Target number of sequences per chunk"),
    ] = 50_000,
) -> None:
    """Ingest and chunk FASTA files for alignment."""
    from distributed_alignment.ingest.chunker import chunk_sequences
    from distributed_alignment.ingest.fasta_parser import parse_fasta
    from distributed_alignment.observability.logging import configure_logging

    run_id = _generate_run_id()
    configure_logging(level="INFO", run_id=run_id, json_output=False)

    output_path = output_dir.resolve()

    # Count sequences to determine chunk count
    typer.echo(f"Counting query sequences from {queries}...")
    q_count = sum(1 for _ in parse_fasta(queries))
    typer.echo(f"Counting reference sequences from {reference}...")
    r_count = sum(1 for _ in parse_fasta(reference))

    q_num_chunks = max(1, q_count // chunk_size + (1 if q_count % chunk_size else 0))
    r_num_chunks = max(1, r_count // chunk_size + (1 if r_count % chunk_size else 0))

    # Chunk queries
    typer.echo(f"Chunking {q_count} queries into {q_num_chunks} chunks...")
    q_chunks_dir = output_path / "chunks" / "queries"
    q_manifest = chunk_sequences(
        parse_fasta(queries),
        num_chunks=q_num_chunks,
        output_dir=q_chunks_dir,
        chunk_prefix="q",
        run_id=run_id,
        input_files=[str(queries)],
    )

    # Chunk references
    typer.echo(f"Chunking {r_count} references into {r_num_chunks} chunks...")
    r_chunks_dir = output_path / "chunks" / "references"
    r_manifest = chunk_sequences(
        parse_fasta(reference),
        num_chunks=r_num_chunks,
        output_dir=r_chunks_dir,
        chunk_prefix="r",
        run_id=run_id,
        input_files=[str(reference)],
    )

    # Write manifests
    q_manifest_path = output_path / "query_manifest.json"
    r_manifest_path = output_path / "ref_manifest.json"
    q_manifest_path.write_text(
        json.dumps(q_manifest.model_dump(mode="json"), indent=2)
    )
    r_manifest_path.write_text(
        json.dumps(r_manifest.model_dump(mode="json"), indent=2)
    )

    typer.echo("")
    typer.echo(f"Ingestion complete (run_id: {run_id})")
    typer.echo(f"  Queries:    {q_count} sequences → {len(q_manifest.chunks)} chunks")
    typer.echo(f"  References: {r_count} sequences → {len(r_manifest.chunks)} chunks")
    typer.echo(f"  Output:     {output_path}")


@app.command()
def run(
    work_dir: Annotated[
        Path,
        typer.Option(help="Working directory containing chunks and manifests"),
    ] = Path("./work"),
    workers: Annotated[
        int,
        typer.Option(help="Number of workers (only 1 supported in Phase 1)"),
    ] = 1,
    sensitivity: Annotated[
        str,
        typer.Option(help="DIAMOND sensitivity mode"),
    ] = "very-sensitive",
    top_n: Annotated[
        int,
        typer.Option(help="Maximum hits per query after merging"),
    ] = 50,
) -> None:
    """Run the alignment pipeline."""
    from distributed_alignment.merge.merger import merge_query_chunk
    from distributed_alignment.models import ChunkManifest
    from distributed_alignment.observability.logging import configure_logging
    from distributed_alignment.scheduler.filesystem_backend import (
        FileSystemWorkStack,
    )
    from distributed_alignment.worker.diamond_wrapper import DiamondWrapper
    from distributed_alignment.worker.runner import WorkerRunner

    work_path = work_dir.resolve()

    # Read manifests
    q_manifest_path = work_path / "query_manifest.json"
    r_manifest_path = work_path / "ref_manifest.json"

    if not q_manifest_path.exists() or not r_manifest_path.exists():
        typer.echo(
            "Error: manifests not found. Run 'distributed-alignment ingest' first.",
            err=True,
        )
        raise typer.Exit(code=1)

    q_manifest = ChunkManifest(**json.loads(q_manifest_path.read_text()))
    r_manifest = ChunkManifest(**json.loads(r_manifest_path.read_text()))

    run_id = q_manifest.run_id
    configure_logging(level="INFO", run_id=run_id, json_output=False)

    if workers > 1:
        typer.echo(
            f"Warning: only 1 worker supported in Phase 1 (requested {workers}). "
            f"Using 1 worker.",
            err=True,
        )

    # Check DIAMOND is available
    diamond = DiamondWrapper(binary="diamond", threads=1)
    if not diamond.check_available():
        typer.echo(
            "Error: DIAMOND binary not found. Install it or use the Docker container.",
            err=True,
        )
        raise typer.Exit(code=1)

    # Generate work packages
    work_stack_dir = work_path / "work_stack"
    stack = FileSystemWorkStack(work_stack_dir)
    packages = stack.generate_work_packages(q_manifest, r_manifest)

    total_packages = len(packages)
    typer.echo(
        f"Generated {total_packages} work packages "
        f"({len(q_manifest.chunks)} query × {len(r_manifest.chunks)} ref chunks)"
    )

    # Run worker
    chunks_dir = work_path / "chunks"
    results_dir = work_path / "results"

    runner = WorkerRunner(
        stack,
        diamond,
        chunks_dir,
        results_dir,
        sensitivity=sensitivity,
        max_target_seqs=top_n,
    )

    typer.echo("Starting alignment...")
    runner.run()

    # Merge results per query chunk
    merged_dir = work_path / "merged"
    ref_chunk_ids = [c.chunk_id for c in r_manifest.chunks]

    typer.echo("Merging results...")
    for q_chunk in q_manifest.chunks:
        merge_query_chunk(
            q_chunk.chunk_id,
            results_dir,
            merged_dir,
            top_n=top_n,
            expected_ref_chunks=ref_chunk_ids,
        )

    # Summary
    stack_status = stack.status()
    typer.echo("")
    typer.echo(f"Pipeline complete (run_id: {run_id})")
    typer.echo(f"  Completed: {stack_status.get('COMPLETED', 0)}/{total_packages}")
    typer.echo(f"  Failed:    {stack_status.get('POISONED', 0)}")
    typer.echo(f"  Results:   {merged_dir}")

    if stack_status.get("POISONED", 0) > 0:
        typer.echo(
            "\nWarning: some packages failed. Check logs for details.",
            err=True,
        )
        raise typer.Exit(code=1)


@app.command()
def status(
    work_dir: Annotated[
        Path,
        typer.Option(help="Working directory to check status of"),
    ] = Path("./work"),
) -> None:
    """Show pipeline run status."""
    from rich.console import Console
    from rich.table import Table

    from distributed_alignment.models import ChunkManifest
    from distributed_alignment.scheduler.filesystem_backend import (
        FileSystemWorkStack,
    )

    work_path = work_dir.resolve()
    console = Console()

    # Read manifests
    q_manifest_path = work_path / "query_manifest.json"
    r_manifest_path = work_path / "ref_manifest.json"

    if not q_manifest_path.exists():
        console.print(
            "[red]No pipeline data found.[/red] "
            "Run 'distributed-alignment ingest' first."
        )
        raise typer.Exit(code=1)

    q_manifest = ChunkManifest(**json.loads(q_manifest_path.read_text()))
    r_manifest = (
        ChunkManifest(**json.loads(r_manifest_path.read_text()))
        if r_manifest_path.exists()
        else None
    )

    console.print(f"\n[bold]Run:[/bold] {q_manifest.run_id}")
    console.print(
        f"[bold]Queries:[/bold] {q_manifest.total_sequences} sequences "
        f"in {len(q_manifest.chunks)} chunks"
    )
    if r_manifest:
        console.print(
            f"[bold]References:[/bold] {r_manifest.total_sequences} sequences "
            f"in {len(r_manifest.chunks)} chunks"
        )

    # Work stack status
    work_stack_dir = work_path / "work_stack"
    if work_stack_dir.exists():
        stack = FileSystemWorkStack(work_stack_dir)
        stack_status = stack.status()

        table = Table(title="Work Packages")
        table.add_column("State", style="bold")
        table.add_column("Count", justify="right")

        for state_name in ["PENDING", "RUNNING", "COMPLETED", "POISONED"]:
            count = stack_status.get(state_name, 0)
            style = {
                "PENDING": "yellow",
                "RUNNING": "blue",
                "COMPLETED": "green",
                "POISONED": "red",
            }.get(state_name, "")
            table.add_row(state_name, f"[{style}]{count}[/{style}]")

        console.print(table)
    else:
        console.print(
            "\n[yellow]No work packages yet.[/yellow] "
            "Run 'distributed-alignment run' to start."
        )

    # Merged results
    merged_dir = work_path / "merged"
    if merged_dir.exists():
        merged_files = list(merged_dir.glob("merged_*.parquet"))
        console.print(
            f"\n[bold]Merged results:[/bold] "
            f"{len(merged_files)} files in {merged_dir}"
        )
    console.print()


@app.command()
def explore(
    port: Annotated[
        int,
        typer.Option(help="Port for the results explorer web UI"),
    ] = 8000,
    data_dir: Annotated[
        str | None,
        typer.Option(help="Directory containing pipeline results"),
    ] = None,
) -> None:
    """Launch the interactive results explorer."""
    typer.echo(
        "Results explorer is not yet implemented — coming in Phase 4.",
        err=True,
    )


if __name__ == "__main__":
    app()
