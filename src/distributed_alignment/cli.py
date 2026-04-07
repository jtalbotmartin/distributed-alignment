"""CLI entry point for distributed-alignment.

Uses Typer for command-line interface with subcommands for each pipeline stage.
"""

from typing import Annotated

import typer

app = typer.Typer(
    name="distributed-alignment",
    help="Distributed, fault-tolerant protein sequence alignment pipeline.",
    no_args_is_help=True,
)


@app.command()
def ingest(
    queries: Annotated[
        str,
        typer.Option(help="Path to query FASTA file"),
    ],
    reference: Annotated[
        str,
        typer.Option(help="Path to reference FASTA file"),
    ],
    output_dir: Annotated[
        str,
        typer.Option(help="Output directory for chunks and manifests"),
    ] = "./work",
) -> None:
    """Ingest and chunk FASTA files for alignment."""
    typer.echo(f"Ingesting queries from {queries} and reference from {reference}")
    typer.echo(f"Output directory: {output_dir}")
    typer.echo("Not yet implemented — see Task 1.1 and 1.2")
    raise typer.Exit(code=1)


@app.command()
def run(
    work_dir: Annotated[
        str,
        typer.Option(help="Working directory containing chunks and manifests"),
    ] = "./work",
    workers: Annotated[
        int,
        typer.Option(help="Number of workers to spawn"),
    ] = 4,
    sensitivity: Annotated[
        str,
        typer.Option(help="DIAMOND sensitivity mode"),
    ] = "very-sensitive",
) -> None:
    """Run the alignment pipeline."""
    typer.echo(f"Running pipeline from {work_dir} with {workers} workers")
    typer.echo(f"Sensitivity: {sensitivity}")
    typer.echo("Not yet implemented — see Tasks 1.3-1.5")
    raise typer.Exit(code=1)


@app.command()
def status(
    work_dir: Annotated[
        str,
        typer.Option(help="Working directory to check status of"),
    ] = "./work",
) -> None:
    """Show pipeline run status."""
    typer.echo(f"Checking status of {work_dir}")
    typer.echo("Not yet implemented — see Task 1.7")
    raise typer.Exit(code=1)


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
    typer.echo(f"Starting explorer on port {port}")
    if data_dir:
        typer.echo(f"Data directory: {data_dir}")
    typer.echo("Not yet implemented — see Phase 4")
    raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
