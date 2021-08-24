"""This implements the Command Line Interface which enables the user to
use the functionality implemented in the :mod:`~digiarch` submodules.
The CLI implements several commands with suboptions.

"""
# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------
import asyncio
from functools import wraps
from pathlib import Path
from typing import Any
from typing import Callable
from typing import List

import click
from click.core import Context
from digiarch import __version__
from digiarch import core
from digiarch.exceptions import FileCollectionError
from digiarch.exceptions import FileParseError
from digiarch.exceptions import IdentificationError
from digiarch.models import FileData
import os

# -----------------------------------------------------------------------------
# Auxiliary functions
# -----------------------------------------------------------------------------


def coro(func: Callable) -> Callable:
    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        return asyncio.run(func(*args, **kwargs))

    return wrapper


# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------


@click.group(invoke_without_command=True, chain=True)
@click.argument(
    "path", type=click.Path(exists=True, file_okay=False, resolve_path=True)
)
@click.option("--reindex", is_flag=True, help="Reindex the current directory.")
@click.version_option(version=__version__)
@click.pass_context
@coro
async def cli(ctx: Context, path: str, reindex: bool) -> None:
    """Used for indexing, reporting on, and identifying files
    found in PATH.
    """

    # Initialise
    in_path: Path = Path(path)
    os.environ["ROOTPATH"] = path
    file_data: FileData = FileData(main_dir=in_path, files=[])
    empty: bool = await file_data.db.is_empty()
    warnings: List[str] = []

    # Collect file info and update file_data
    if reindex or empty:
        click.secho("Collecting file information...", bold=True)
        try:
            warnings = await core.explore_dir(file_data)
        except FileCollectionError as error:
            raise click.ClickException(str(error))

    else:
        click.echo("Processing data from ", nl=False)
        click.secho(f"{file_data.db.url}", bold=True)

    for warning in warnings:
        click.secho(warning, bold=True, fg="red")

    try:
        file_data.files = await file_data.db.get_files()
    except FileParseError as error:
        raise click.ClickException(str(error))
    else:
        ctx.obj = file_data
        _files = file_data.files
        _files = core.generate_checksums(_files)
        try:
            print("Identifying")
            _files = core.identify(_files, file_data.main_dir)
        except IdentificationError as error:
            raise click.ClickException(str(error))
        else:
            click.secho(f"Successfully identified {len(_files)} files.")
            print("Finished identifying")
            file_data.files = _files


@cli.command()
@click.pass_obj
def process(file_data: FileData) -> None:
    """Generate checksums and identify files."""
    print("Generate checksums")
    _files = file_data.files
    _files = core.generate_checksums(_files)
    click.secho("Identifying files... ", nl=False)
    try:
        print("Identifying")
        _files = core.identify(_files, file_data.main_dir)
    except IdentificationError as error:
        raise click.ClickException(str(error))
    else:
        #   click.secho(f"Successfully identified {len(_files)} files.")
        print("Finished identifying")
        file_data.files = _files


@cli.command()
@click.pass_context
@coro
async def fix(ctx: Context) -> None:
    """Fix file extensions - files should be identified first."""
    file_data = ctx.obj
    fixed = core.fix_extensions(file_data.files)
    if fixed:
        click.secho("Rebuilding file information...", bold=True)
        new_files = core.identify(fixed, file_data.main_dir)
        await file_data.db.update_files(new_files)
        file_data.files = await file_data.db.get_files()
        ctx.obj = file_data
    else:
        click.secho("Info: No file extensions to fix.", bold=True, fg="yellow")


@cli.resultcallback()
@coro
async def done(result: Any, **kwargs: Any) -> None:
    ctx = click.get_current_context()
    file_data: FileData = ctx.obj
    await file_data.db.set_files(file_data.files)
    click.secho("Done!", bold=True, fg="green")


if __name__ == "__main__":
    cli()
