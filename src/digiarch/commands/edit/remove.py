from logging import INFO
from logging import Logger
from pathlib import Path
from sqlite3 import DatabaseError
from typing import Literal

from acacore.database import FilesDB
from acacore.database.table import Table
from acacore.models.event import Event
from acacore.models.file import BaseFile
from acacore.models.file import MasterFile
from acacore.models.file import OriginalFile
from acacore.utils.click import end_program
from acacore.utils.click import start_program
from acacore.utils.helpers import ExceptionManager
from click import argument
from click import command
from click import Context
from click import option
from click import pass_context
from digiarch.__version__ import __version__
from digiarch.common import AVID
from digiarch.common import CommandWithRollback
from digiarch.common import get_avid
from digiarch.common import open_database
from digiarch.common import option_dry_run
from digiarch.common import rollback
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
    log_removal: bool = True,
):
    file.get_absolute_path(avid.path).unlink(missing_ok=True)
    remove_empty_dir(avid.path, file.get_absolute_path(avid.path).parent)
    table.delete(file)
    if log_removal:
        event = Event.from_command(ctx, "delete", (file.uuid, file_type), file.model_dump(mode="json"))
        db.log.insert(event)
        event.log(INFO, *loggers, show_args=["uuid"], path=file.relative_path)


def remove_children(
    ctx: Context,
    avid: AVID,
    db: FilesDB,
    file: BaseFile,
    *loggers: Logger,
    log_removal: bool = True,
) -> None:
    if isinstance(file, OriginalFile):
        for child in db.master_files.select({"original_uuid": str(file.uuid)}):
            remove_children(ctx, avid, db, child, *loggers, log_removal=log_removal)
            remove_child(ctx, avid, db, db.master_files, child, "master", *loggers, log_removal=log_removal)
    elif isinstance(file, MasterFile):
        for child in db.access_files.select({"original_uuid": str(file.uuid)}):
            remove_child(ctx, avid, db, db.access_files, child, "access", *loggers, log_removal=log_removal)
        for child in db.statutory_files.select({"original_uuid": str(file.uuid)}):
            remove_child(ctx, avid, db, db.statutory_files, child, "statutory", *loggers, log_removal=log_removal)


def remove_files(
    ctx: Context,
    avid: AVID,
    database: FilesDB,
    table: Table[BaseFile],
    query: TQuery,
    file_type: Literal["original", "master", "access", "statutory"],
    reason: str,
    delete: bool,
    reset_processed: bool,
    dry_run: bool,
    *loggers: Logger,
) -> None:
    while files := list(query_table(table, query, [("lower(relative_path)", "asc")], 100)):
        for file in files:
            event = Event.from_command(
                ctx,
                "delete" if delete else "remove",
                (file.uuid, file_type),
                file.model_dump(mode="json"),
                reason,
            )

            event.log(INFO, *loggers, show_args=["uuid"], path=file.relative_path)

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

            database.commit()


def rollback_remove_original(_ctx: Context, avid: AVID, database: FilesDB, event: Event, file: BaseFile | None):
    old_file = OriginalFile.model_validate(event.data)
    old_file.root = avid.path

    if file and file.relative_path != old_file.relative_path:
        # File exists in database but the path is different
        raise DatabaseError(f"Duplicated file UUID {file.uuid}")
    if not old_file.get_absolute_path().is_file():
        raise FileNotFoundError(old_file.relative_path)

    database.original_files.insert(old_file)
    database.commit()


@rollback("remove", rollback_remove_original)
@command("remove", no_args_is_help=True, short_help="Remove files.", cls=CommandWithRollback)
@argument_query(True, "uuid", ["uuid", "checksum", "puid", "relative_path", "action", "warning", "processed", "lock"])
@argument("reason", nargs=1, type=str, required=True)
@option("--delete", is_flag=True, default=False, help="Remove selected files from the disk.")
@option_dry_run()
@pass_context
def cmd_remove_original(
    ctx: Context,
    reason: str,
    query: TQuery,
    delete: bool,
    dry_run: bool,
):
    """
    Remove one or more files in OriginalDocuments matching the QUERY argument.

    Using the --delete option removes the files from the disk.

    All children files (master, access, and statutory) are removed recursively from both the database and the disk regardless of the --delete option.

    To see the changes without committing them, use the --dry-run option.

    For details on the QUERY argument, see the edit command.
    """
    avid = get_avid(ctx)

    with open_database(ctx, avid) as database:
        log_file, log_stdout, _ = start_program(ctx, database, __version__, None, True, True, dry_run)

        with ExceptionManager(BaseException) as exception:
            remove_files(
                ctx,
                avid,
                database,
                database.original_files,
                query,
                "original",
                reason,
                delete,
                False,
                dry_run,
                log_stdout,
            )

        end_program(ctx, database, exception, dry_run, log_file, log_stdout)


@command("remove", no_args_is_help=True, short_help="Remove files.")
@argument_query(True, "uuid", ["uuid", "checksum", "puid", "relative_path", "action", "warning", "processed"])
@argument("reason", nargs=1, type=str, required=True)
@option("--reset-processed", is_flag=True, default=False, help="Reset processed status of parent files.")
@option_dry_run()
@pass_context
def cmd_remove_master(
    ctx: Context,
    reason: str,
    query: TQuery,
    reset_processed: bool,
    dry_run: bool,
):
    """
    Remove one or more files in MasterDocuments matching the QUERY argument.

    All matching files and their children (access, and statutory) are removed recursively from both the database and the disk.

    To set the parent files in OriginalDocuments to unprocessed, use the --reset-processed option.

    To see the changes without committing them, use the --dry-run option.

    For details on the QUERY argument, see the edit command.
    """
    avid = get_avid(ctx)

    with open_database(ctx, avid) as database:
        log_file, log_stdout, _ = start_program(ctx, database, __version__, None, True, True, dry_run)

        with ExceptionManager(BaseException) as exception:
            remove_files(
                ctx,
                avid,
                database,
                database.master_files,
                query,
                "master",
                reason,
                True,
                reset_processed,
                dry_run,
                log_stdout,
            )

        end_program(ctx, database, exception, dry_run, log_file, log_stdout)


@command("remove", no_args_is_help=True, short_help="Remove files.")
@argument_query(True, "uuid", ["uuid", "checksum", "puid", "relative_path", "action", "warning"])
@argument("reason", nargs=1, type=str, required=True)
@option("--reset-processed", is_flag=True, default=False, help="Reset processed status of parent files.")
@option_dry_run()
@pass_context
def cmd_remove_access(
    ctx: Context,
    reason: str,
    query: TQuery,
    reset_processed: bool,
    dry_run: bool,
):
    """
    Remove one or more files in AccessDocuments matching the QUERY argument.

    All matching files are removed from both the database and the disk.

    To set the parent files in MasterDocuments to unprocessed, use the --reset-processed option.

    To see the changes without committing them, use the --dry-run option.

    For details on the QUERY argument, see the edit command.
    """
    avid = get_avid(ctx)

    with open_database(ctx, avid) as database:
        log_file, log_stdout, _ = start_program(ctx, database, __version__, None, True, True, dry_run)

        with ExceptionManager(BaseException) as exception:
            remove_files(
                ctx,
                avid,
                database,
                database.access_files,
                query,
                "access",
                reason,
                True,
                reset_processed,
                dry_run,
                log_stdout,
            )

        end_program(ctx, database, exception, dry_run, log_file, log_stdout)


@command("remove", no_args_is_help=True, short_help="Remove files.")
@argument_query(True, "uuid", ["uuid", "checksum", "puid", "relative_path", "action", "warning"])
@argument("reason", nargs=1, type=str, required=True)
@option("--reset-processed", is_flag=True, default=False, help="Reset processed status of parent files.")
@option_dry_run()
@pass_context
def cmd_remove_statutory(
    ctx: Context,
    reason: str,
    query: TQuery,
    reset_processed: bool,
    dry_run: bool,
):
    """
    Remove one or more files in Documents matching the QUERY argument.

    All matching files are removed from both the database and the disk.

    To set the parent files in MasterDocuments to unprocessed, use the --reset-processed option.

    To see the changes without committing them, use the --dry-run option.

    For details on the QUERY argument, see the edit command.
    """
    avid = get_avid(ctx)

    with open_database(ctx, avid) as database:
        log_file, log_stdout, _ = start_program(ctx, database, __version__, None, True, True, dry_run)

        with ExceptionManager(BaseException) as exception:
            remove_files(
                ctx,
                avid,
                database,
                database.statutory_files,
                query,
                "statutory",
                reason,
                True,
                reset_processed,
                dry_run,
                log_stdout,
            )

        end_program(ctx, database, exception, dry_run, log_file, log_stdout)
