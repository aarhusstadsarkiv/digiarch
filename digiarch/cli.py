"""This implements the Command Line Interface which enables the user to
use the functionality implemented in the :mod:`~digiarch` submodules.
The CLI implements several commands with suboptions.

"""

# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------

from datetime import datetime
from pathlib import Path

import click

from digiarch.exceptions import FileCollectionError
from digiarch.identify import checksums, identify_files, reports
from digiarch.internals import FileData, Metadata
from digiarch.utils import group_files, path_utils

# -----------------------------------------------------------------------------
# Function Definitions
# -----------------------------------------------------------------------------


@click.group(invoke_without_command=True, chain=True)
@click.argument(
    "path", type=click.Path(exists=True, file_okay=False, resolve_path=True)
)
@click.option(
    "--reindex", is_flag=True, help="Whether to reindex the current directory."
)
@click.pass_context
def cli(ctx: click.core.Context, path: str, reindex: bool) -> None:
    """Used for indexing, reporting on, and identifying files
    found in PATH.
    """

    # Initialise FileData
    metadata = Metadata(last_run=datetime.now(), processed_dir=Path(path))
    init_file_data = FileData(metadata)

    # Collect file info and update file_data
    if reindex or init_file_data.json_file.stat().st_size == 0:
        click.secho("Collecting file information...", bold=True)
        try:
            file_data = path_utils.explore_dir(Path(path))
        except FileCollectionError as error:
            raise click.ClickException(str(error))
        else:
            if file_data.metadata.empty_subdirs:
                click.secho(
                    "Warning! Empty subdirectories detected!",
                    bold=True,
                    fg="red",
                )
            if file_data.metadata.several_files:
                click.secho(
                    "Warning! Some directories have several files!",
                    bold=True,
                    fg="red",
                )
        click.secho("Done!", bold=True, fg="green")
    else:
        click.echo("Processing data from ", nl=False)
        click.secho(f"{init_file_data.json_file}", bold=True)
        file_data = FileData.from_json(init_file_data.json_file)

    ctx.obj = file_data


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
def checksum(file_data: FileData) -> None:
    """Generate file checksums using SHA-256."""
    file_data.files = checksums.generate_checksums(file_data.files)
    file_data.to_json()


@cli.command()
@click.pass_obj
def dups(file_data: FileData) -> None:
    """Check for file duplicates."""
    checksums.check_duplicates(file_data.files, file_data.digiarch_dir)


@cli.command()
@click.pass_obj
def identify(file_data: FileData) -> None:
    """Identify files using siegfried."""
    file_data.files = identify_files.identify(
        file_data.files, file_data.metadata.processed_dir
    )
    file_data.to_json()


@cli.resultcallback()
def done(result: Any, **kwargs: Any) -> None:
    click.secho("Done!", bold=True, fg="green")
