from logging import INFO
from pathlib import Path
from re import match
from sqlite3 import Error as SQLiteError

from acacore.database import FilesDB
from acacore.models.event import Event
from acacore.models.file import BaseFile
from acacore.models.file import OriginalFile
from acacore.utils.click import end_program
from acacore.utils.click import param_callback_regex
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


def rollback_rename_original(_ctx: Context, avid: AVID, database: FilesDB, event: Event, file: BaseFile | None):
    if not file:
        raise FileNotFoundError(f"No file with UUID {event.file_uuid}")
    if not isinstance(file, OriginalFile):
        raise TypeError(f"{type(file)} is not OriginalFile")

    file.root = avid.path
    current_path: Path = file.relative_path
    prev_name: str
    prev_name, _ = event.data

    if file.relative_path == file.relative_path.with_name(prev_name):
        return
    if file.get_absolute_path().with_name(prev_name).is_file():
        raise FileExistsError(f"File with name {prev_name!r} already exists")

    file.get_absolute_path().rename(file.get_absolute_path().with_name(prev_name))
    file.relative_path = file.relative_path.with_name(prev_name)

    try:
        database.original_files.update(file, {"relative_path": str(current_path)})
    except:
        file.get_absolute_path().rename(file.get_absolute_path().with_name(current_path.name))
        raise


@rollback("edit", rollback_rename_original)
@command("rename", no_args_is_help=True, short_help="Change file extensions.", cls=CommandWithRollback)
@argument_query(True, "uuid", ["uuid", "checksum", "puid", "relative_path", "action", "warning", "processed", "lock"])
@argument(
    "extension",
    nargs=1,
    type=str,
    required=True,
    callback=param_callback_regex(r"^((\.[a-zA-Z0-9]+)+| +)$"),
)
@argument("reason", nargs=1, type=str, required=True)
@option("--append", "replace_mode", flag_value="append", default=True, help="Append the new extension.  [default]")
@option("--replace", "replace_mode", flag_value="last", help="Replace the last extension.")
@option("--replace-all", "replace_mode", flag_value="all", help="Replace all extensions.")
@option_dry_run()
@pass_context
def cmd_rename_original(
    ctx: Context,
    query: TQuery,
    reason: str,
    extension: str,
    replace_mode: str,
    dry_run: bool,
):
    r"""
    Change the extension of one or more files in OriginalDocuments matching the QUERY argument to EXTENSION.

    To see the changes without committing them, use the --dry-run option.

    The --replace and --replace-all options will only replace valid suffixes (i.e., matching the expression \.[a-zA-Z0-9]+).

    The --append option will not add the new extension if it is already present.
    """
    extension = extension.strip()

    avid = get_avid(ctx)

    with open_database(ctx, avid) as database:
        log_file, log_stdout, _ = start_program(ctx, database, __version__, None, True, True, dry_run)

        with ExceptionManager(BaseException) as exception:
            for file in query_table(database.original_files, query, [("lower(relative_path)", "asc")]):
                old_name: str = file.name
                new_name: str = old_name

                if replace_mode == "last" and not match(r"^\.[a-zA-Z0-9]+$", file.relative_path.suffix):
                    new_name = file.name + extension
                elif replace_mode == "last":
                    new_name = file.relative_path.with_suffix(extension).name
                elif replace_mode == "append" and old_name.lower().endswith(extension.lower()):
                    new_name = old_name
                elif replace_mode == "append":
                    new_name = file.name + extension
                elif replace_mode == "all":
                    new_name = file.name.removesuffix(file.suffixes) + extension

                if new_name.lower() == old_name.lower():
                    Event.from_command(ctx, "skip", (file.uuid, "original"), [str(old_name), str(new_name)]).log(
                        INFO,
                        log_stdout,
                        show_args=["uuid", "data"],
                    )
                    continue

                event = Event.from_command(ctx, "edit", (file.uuid, "original"), [str(old_name), str(new_name)], reason)

                if dry_run:
                    event.log(INFO, log_stdout, show_args=["uuid", "data"])
                    continue

                file.root = avid.path
                file.get_absolute_path().rename(file.get_absolute_path().with_name(new_name))
                file.relative_path = file.relative_path.with_name(new_name)

                try:
                    database.original_files.update(file, {"uuid": str(file.uuid)})
                except SQLiteError:
                    file.get_absolute_path().rename(file.get_absolute_path().with_name(old_name))
                    raise

                event.log(INFO, log_stdout, show_args=["uuid", "data"])
                database.log.insert(event)
                database.commit()

        end_program(ctx, database, exception, dry_run, log_file, log_stdout)
