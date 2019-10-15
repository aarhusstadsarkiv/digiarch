"""Module level documentation
Describe the module. Be precise.
End with a blank line before the last triple quote.

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
def cli():
    """Command Line Tool for handling Aarhus Digital Archive handins."""
    pass


@cli.command()
@click.option("--path", type=click.Path(exists=True, file_okay=False))
def report(path):
    file_exts, empty_dirs = path_utils.explore_dir(path)
    reports.report_results(file_exts, empty_dirs, path)
