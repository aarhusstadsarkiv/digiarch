"""This implements the Command Line Interface which enables the user to
use the functionality implemented in the `digital_archive` submodules.
The CLI implements several commands with suboptions.

"""

# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------
import click
from .utils import path_utils
from .identify import reports

# -----------------------------------------------------------------------------
# Function Definitions
# -----------------------------------------------------------------------------


@click.group()
def cli() -> None:
    """Command Line Tool for handling Aarhus Digital Archive handins."""
    pass


@cli.command()
@click.option(
    "--path", type=click.Path(exists=True, file_okay=False, resolve_path=True)
)
def report(path: str) -> None:
    """This command invokes `report_results` on files and
    subdirectories found in the given `--path`."""
    # TODO: --path should be optional, default to directory where
    # the CLI is called.
    # TODO: Check if path is empty, exit gracefully if so.
    click.secho("Collecting file information...", bold=True)
    dir_info = path_utils.explore_dir(path)
    click.secho("Done!", bold=True, fg="green")
    reports.report_results(dir_info, path)
