from logging import INFO
from logging import Logger
from pathlib import Path
from typing import Literal

from acacore.database import FilesDB
from acacore.database.table import Table
from acacore.models.event import Event
from acacore.models.file import BaseFile
from acacore.models.file import ConvertedFile
from acacore.models.file import MasterFile
from acacore.models.file import OriginalFile
from acacore.utils.click import ctx_params
from acacore.utils.click import end_program
from acacore.utils.click import start_program
from acacore.utils.helpers import ExceptionManager
from click import argument
from click import BadParameter
from click import Choice
from click import command
from click import Context
from click import option
from click import pass_context

from digiarch.__version__ import __version__
from digiarch.common import AVID
from digiarch.common import get_avid
from digiarch.common import open_database
from digiarch.common import option_dry_run
from digiarch.query import argument_query
from digiarch.query import query_table
from digiarch.query import TQuery


def remove_empty_dir(root: Path, path: Path) -> None:
    if path == root:
        return None
    elif not path.is_relative_to(root):
        return None
    elif not path.is_dir():
        return None
    elif next(path.iterdir(), None):
        return None

    path.rmdir()

    return remove_empty_dir(root, path.parent)


def reset_parent_processed(db: FilesDB, file: BaseFile):
    parent_table: Table[OriginalFile | MasterFile]

    if isinstance(file, OriginalFile):
        return

    if isinstance(file, MasterFile):  # noqa: SIM108
        parent_table = db.original_files
    else:
        parent_table = db.master_files

    if (parent_file := parent_table[{"uuid": str(file.original_uuid)}]) and parent_file.processed:
        parent_file.processed = False
        parent_table.update(parent_file)


def remove_child(
    ctx: Context,
    avid: AVID,
    db: FilesDB,
    table: Table[BaseFile],
    file: BaseFile,
    file_type: Literal["original", "master", "access", "statutory"],
    *loggers: Logger,
):
    file.get_absolute_path(avid.path).unlink(missing_ok=True)
    remove_empty_dir(avid.path, file.get_absolute_path(avid.path).parent)
    table.delete(file)
    event = Event.from_command(ctx, "delete", (file.uuid, file_type), file.model_dump(mode="json"))
    db.log.insert(event)
    event.log(INFO, *loggers, show_args=["uuid"], path=file.relative_path)


def remove_children(ctx: Context, avid: AVID, db: FilesDB, file: BaseFile, *loggers: Logger) -> None:
    if isinstance(file, OriginalFile):
        for child in db.master_files.select({"original_uuid": str(file.uuid)}):
            remove_child(ctx, avid, db, db.master_files, child, "master", *loggers)
            remove_children(ctx, avid, db, child)
    elif isinstance(file, MasterFile):
        for child in db.access_files.select({"original_uuid": str(file.uuid)}):
            remove_child(ctx, avid, db, db.access_files, child, "access", *loggers)
        for child in db.statutory_files.select({"original_uuid": str(file.uuid)}):
            remove_child(ctx, avid, db, db.statutory_files, child, "statutory", *loggers)


@command("remove", no_args_is_help=True, short_help="Remove files.")
@argument("file_type", type=Choice(["original", "master", "access", "statutory"]), required=True)
@argument_query(True, "uuid", ["uuid", "checksum", "puid", "relative_path", "action", "warning", "processed", "lock"])
@argument("reason", nargs=1, type=str, required=True)
@option("--delete", is_flag=True, default=False, help="Remove selected files from the disk.")
@option("--reset-processed", is_flag=True, default=False, help="Reset processed status of parent files.")
@option_dry_run()
@pass_context
def command_remove(
    ctx: Context,
    file_type: Literal["original", "master"],
    reason: str,
    query: TQuery,
    delete: bool,
    reset_processed: bool,
    dry_run: bool,
):
    """
    Remove one or more files in the files' database for the ROOT folder to EXTENSION.

    Using the --delete option removes the files from the disk.

    To see the changes without committing them, use the --dry-run option.

    For details on the QUERY argument, see the edit command.
    """
    avid = get_avid(ctx)

    with open_database(ctx, avid) as database:
        log_file, log_stdout, _ = start_program(ctx, database, __version__, None, True, True, dry_run)
        table: Table[OriginalFile | ConvertedFile]

        if file_type == "original":
            table = database.original_files
        elif file_type == "master":
            table = database.master_files
        elif file_type == "access":
            table = database.access_files
        elif file_type == "statutory":
            table = database.statutory_files
        else:
            raise BadParameter(f"invalid file type {file_type!r}", ctx, ctx_params(ctx)["file_type"])

        with ExceptionManager(BaseException) as exception:
            while files := list(query_table(table, query, [("lower(relative_path)", "asc")], 100)):
                for file in files:
                    event = Event.from_command(
                        ctx,
                        "delete" if delete else "remove",
                        (file.uuid, file_type),
                        file.model_dump(mode="json"),
                        reason,
                    )

                    event.log(INFO, log_stdout, show_args=["uuid"], path=file.relative_path)

                    if dry_run:
                        continue

                    table.delete(file)
                    database.log.insert(event)

                    if delete:
                        file.root = avid.path
                        file.get_absolute_path().unlink(missing_ok=True)
                        remove_empty_dir(avid.path, file.get_absolute_path().parent)

                    remove_children(ctx, avid, database, file)

                    if reset_processed:
                        reset_parent_processed(database, file)

        end_program(ctx, database, exception, dry_run, log_file, log_stdout)
