"""This implements the Command Line Interface which enables the user to
use the functionality implemented in the :mod:`~digiarch` submodules.
The CLI implements several commands with suboptions.

"""

# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------
import click
from pathlib import Path
from digiarch.data import get_fileinfo_list, to_json
from digiarch.utils import path_utils, group_files
from digiarch.identify import checksums, reports, identify_files
from digiarch.utils.exceptions import FileCollectionError

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

    # Create directories
    main_dir: Path = Path(path, "_digiarch")
    data_dir: Path = Path(main_dir, ".data")
    data_file: Path = Path(data_dir, "data.json")
    path_utils.create_folders(main_dir, data_dir)

    # Collect file info
    if reindex or not data_file.is_file():
        click.secho("Collecting file information...", bold=True)
        try:
            empty_subs, several_files = path_utils.explore_dir(
                Path(path), main_dir, data_file
            )
        except FileCollectionError as error:
            raise click.ClickException(str(error))
        else:
            if empty_subs:
                click.secho(
                    "Warning! Empty subdirectories detected!",
                    bold=True,
                    fg="red",
                )
            if several_files:
                click.secho(
                    "Warning! Some directories have several files!",
                    bold=True,
                    fg="red",
                )
        click.secho("Done!", bold=True, fg="green")
    else:
        click.echo(f"Processing data from ", nl=False)
        click.secho(f"{data_file}", bold=True)

    ctx.obj = {"main_dir": main_dir, "data_file": data_file}


@cli.command()
@click.pass_obj
def report(path_info: dict) -> None:
    """Generate reports on files and directory structure."""
    reports.report_results(path_info["data_file"], path_info["main_dir"])
    click.secho("Done!", bold=True, fg="green")


@cli.command()
@click.pass_obj
def group(path_info: dict) -> None:
    """Generate lists of files grouped per file extension."""
    group_files.grouping(path_info["data_file"], path_info["main_dir"])
    click.secho("Done!", bold=True, fg="green")


@cli.command()
@click.pass_obj
def checksum(path_info: dict) -> None:
    """Generate file checksums using BLAKE2."""
    files = get_fileinfo_list(path_info["data_file"])
    updated_files = checksums.generate_checksums(files)
    to_json(updated_files, path_info["data_file"])
    click.secho("Done!", bold=True, fg="green")


@cli.command()
@click.pass_obj
def dups(path_info: dict) -> None:
    """Check for file duplicates."""
    files = get_fileinfo_list(path_info["data_file"])
    checksums.check_duplicates(files, path_info["main_dir"])
    click.secho("Done!", bold=True, fg="green")


@cli.command()
@click.pass_obj
def identify(path_info: dict) -> None:
    """Identify files using siegfried."""
    files = get_fileinfo_list(path_info["data_file"])
    updated_files = identify_files.identify(files)
    to_json(updated_files, path_info["data_file"])
    click.secho("Done!", bold=True, fg="green")
