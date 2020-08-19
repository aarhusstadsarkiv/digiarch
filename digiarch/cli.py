"""This implements the Command Line Interface which enables the user to
use the functionality implemented in the :mod:`~digiarch` submodules.
The CLI implements several commands with suboptions.

"""

# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------

from datetime import datetime
from pathlib import Path
from typing import Any

import click
from click.core import Context

from digiarch.exceptions import FileCollectionError
from digiarch.identify import checksums, identify_files, reports
from digiarch.internals import FileData, Metadata
from digiarch.utils import fix_file_exts, group_files, path_utils

# -----------------------------------------------------------------------------
# Function Definitions
# -----------------------------------------------------------------------------


@click.group(invoke_without_command=True, chain=True)
@click.argument(
    "path", type=click.Path(exists=True, file_okay=False, resolve_path=True)
)
@click.option("--reindex", is_flag=True, help="Reindex the current directory.")
@click.option("--all", is_flag=True, help="Run all commands.")
@click.pass_context
def cli(ctx: Context, path: str, reindex: bool, all: bool) -> None:
    """Used for indexing, reporting on, and identifying files
    found in PATH.
    """

    # Initialise FileData
    metadata = Metadata(last_run=datetime.now(), processed_dir=Path(path))
    init_file_data = FileData(metadata=metadata)

    # Collect file info and update file_data
    if reindex or init_file_data.json_file.stat().st_size == 0:
        click.secho("Collecting file information...", bold=True)
        try:
            file_data = path_utils.explore_dir(Path(path))
        except FileCollectionError as error:
            raise click.ClickException(str(error))

    else:
        click.echo("Processing data from ", nl=False)
        click.secho(f"{init_file_data.json_file}", bold=True)
        file_data = FileData.from_json(init_file_data.json_file)

    if file_data.metadata.empty_subdirs:
        click.secho(
            "Warning! Empty subdirectories detected!", bold=True, fg="red",
        )
    if file_data.metadata.several_files:
        click.secho(
            "Warning! Some directories have several files!",
            bold=True,
            fg="red",
        )
    ctx.obj = file_data
    if all:
        ctx.invoke(checksum)
        ctx.invoke(identify)
        ctx.invoke(report)
        ctx.invoke(group)
        ctx.invoke(dups)
        ctx.exit()


@cli.command()
@click.pass_obj
def checksum(file_data: FileData) -> None:
    """Generate file checksums using SHA-256."""
    file_data.files = checksums.generate_checksums(file_data.files)
    file_data.dump()


@cli.command()
@click.pass_obj
def identify(file_data: FileData) -> None:
    """Identify files using siegfried."""
    click.secho("Identifying files... ", nl=False)
    file_data.files = identify_files.identify(
        file_data.files, file_data.metadata.processed_dir
    )
    file_data.dump()
    click.secho(f"Successfully identified {len(file_data.files)} files.")


@cli.command()
@click.pass_obj
def report(file_data: FileData) -> None:
    """Generate reports on files and directory structure."""
    reports.report_results(file_data.files, file_data.digiarch_dir)


@cli.command()
@click.pass_obj
def group(file_data: FileData) -> None:
    """Generate lists of files grouped per file extension."""
    group_files.grouping(file_data.files, file_data.digiarch_dir)


@cli.command()
@click.pass_obj
def dups(file_data: FileData) -> None:
    """Check for file duplicates."""
    checksums.check_duplicates(file_data.files, file_data.digiarch_dir)


@cli.command()
@click.pass_obj
def fix(file_data: FileData) -> None:
    """Fix file extensions"""
    fix_file_exts.fix_extensions(file_data.files)
    click.secho("Rebuilding file information...", bold=True)
    file_data = path_utils.explore_dir(Path(file_data.metadata.processed_dir))
    file_data.dump()


@cli.resultcallback()
def done(result: Any, **kwargs: Any) -> None:
    click.secho("Done!", bold=True, fg="green")
