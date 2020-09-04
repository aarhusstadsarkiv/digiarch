"""This implements the Command Line Interface which enables the user to
use the functionality implemented in the :mod:`~digiarch` submodules.
The CLI implements several commands with suboptions.

"""

# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------

import asyncio
from datetime import datetime
from functools import wraps
from pathlib import Path
from typing import Any, Callable, List, Dict

import click
from click.core import Context
from pydantic import Field, root_validator

from digiarch.database import FileDB
from digiarch.exceptions import FileCollectionError
from digiarch.identify import checksums, identify_files  # , reports
from digiarch.internals import Metadata, ArchiveFile, ACABase
from digiarch.utils import fix_file_exts, group_files, path_utils

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


class FileData(ACABase):
    main_dir: Path
    data_dir: Path = Field(None)
    db: FileDB
    files: List[ArchiveFile]

    class Config:
        arbitrary_types_allowed = True

    @root_validator
    def create_dir(cls, fields: Dict[Any, Any]) -> Dict[Any, Any]:
        main_dir = fields.get("main_dir")
        data_dir = fields.get("data_dir")
        if data_dir is None and main_dir:
            data_dir = main_dir / "_digiarch"
            data_dir.mkdir(exist_ok=True)
            fields["data_dir"] = data_dir
        return fields


@click.group(invoke_without_command=True, chain=True)
@click.argument(
    "path", type=click.Path(exists=True, file_okay=False, resolve_path=True)
)
@click.option("--reindex", is_flag=True, help="Reindex the current directory.")
@click.option("--all", is_flag=True, help="Run all commands.")
@click.pass_context
@coro
async def cli(ctx: Context, path: str, reindex: bool, all: bool) -> None:
    """Used for indexing, reporting on, and identifying files
    found in PATH.
    """

    # Initialise FileDB
    file_db: FileDB = FileDB(fr"sqlite:///{path}\test.db")
    empty = await file_db.is_empty()

    # Collect file info and update file_data
    if reindex or empty:
        click.secho("Collecting file information...", bold=True)
        try:
            await path_utils.explore_dir(Path(path), file_db)
        except FileCollectionError as error:
            raise click.ClickException(str(error))

    else:
        click.echo("Processing data from ", nl=False)
        click.secho(f"{file_db.url}", bold=True)

    # if file_data.metadata.empty_subdirs:
    #     click.secho(
    #         "Warning! Empty subdirectories detected!", bold=True, fg="red",
    #     )
    # if file_data.metadata.several_files:
    #     click.secho(
    #         "Warning! Some directories have several files!",
    #         bold=True,
    #         fg="red",
    #     )
    files: List[ArchiveFile] = await file_db.get_files()
    ctx.obj = FileData(main_dir=path, db=file_db, files=files)
    # if all:
    #     await ctx.invoke(checksum)
    #     # ctx.invoke(identify)
    #     # ctx.invoke(report)
    #     # ctx.invoke(group)
    #     # ctx.invoke(dups)
    #     ctx.exit()


@cli.command()
@click.pass_obj
def checksum(file_data: FileData) -> None:
    """Generate file checksums using SHA-256."""
    file_data.files = checksums.generate_checksums(file_data.files)


# @cli.command()
# @click.pass_obj
# @coro
# async def identify(file_db: FileDB) -> None:
#     """Identify files using siegfried."""
#     click.secho("Identifying files... ", nl=False)
#     files = await file_db.get_files()
#     new_files = identify_files.identify(
#         files, file_data.metadata.processed_dir
#     )
#     file_data.dump()
#     click.secho(f"Successfully identified {len(file_data.files)} files.")


# @cli.command()
# @click.pass_obj
# def report(file_data: FileData) -> None:
#     """Generate reports on files and directory structure."""
#     # reports.report_results(file_data.files, file_data.digiarch_dir)


# @cli.command()
# @click.pass_obj
# def group(file_data: FileData) -> None:
#     """Generate lists of files grouped per file extension."""
#     group_files.grouping(file_data.files, file_data.digiarch_dir)


# @cli.command()
# @click.pass_obj
# def dups(file_data: FileData) -> None:
#     """Check for file duplicates."""
#     checksums.check_duplicates(file_data.files, file_data.digiarch_dir)


# @cli.command()
# @click.pass_obj
# def fix(file_data: FileData) -> None:
#     """Fix file extensions"""
#     fix_file_exts.fix_extensions(file_data.files)
#     click.secho("Rebuilding file information...", bold=True)
#     file_data = path_utils.explore_dir(Path(file_data.metadata.processed_dir))
#     file_data.dump()


@cli.resultcallback()
@coro
async def done(result: Any, **kwargs: Any) -> None:
    ctx = click.get_current_context()
    file_data: FileData = ctx.obj
    await file_data.db.set_files(file_data.files)
    click.secho("Done!", bold=True, fg="green")
