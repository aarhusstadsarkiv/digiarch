from logging import INFO
from pathlib import Path
from re import match
from sqlite3 import Error as SQLiteError

from acacore.database import FileDB
from acacore.models.history import HistoryEntry
from acacore.utils.helpers import ExceptionManager
from click import argument
from click import command
from click import Context
from click import option
from click import pass_context

from digiarch.common import argument_root
from digiarch.common import check_database_version
from digiarch.common import ctx_params
from digiarch.common import end_program
from digiarch.common import option_dry_run
from digiarch.common import param_regex
from digiarch.common import start_program

from .common import argument_query
from .common import find_files
from .common import TQuery


@command("rename", no_args_is_help=True, short_help="Change file extensions.")
@argument_root(True)
@argument_query(True, "uuid", ["uuid", "checksum", "puid", "relative_path", "action", "warning", "processed", "lock"])
@argument(
    "extension",
    nargs=1,
    type=str,
    required=True,
    callback=param_regex(r"^((\.[a-zA-Z0-9]+)+| +)$"),
)
@argument("reason", nargs=1, type=str, required=True)
@option("--append", "replace_mode", flag_value="append", default=True, help="Append the new extension.  [default]")
@option("--replace", "replace_mode", flag_value="last", help="Replace the last extension.")
@option("--replace-all", "replace_mode", flag_value="all", help="Replace all extensions.")
@option_dry_run()
@pass_context
def command_rename(
    ctx: Context,
    root: Path,
    reason: str,
    query: TQuery,
    extension: str,
    replace_mode: str,
    dry_run: bool,
):
    r"""
    Change the extension of one or more files in the files' database for the ROOT folder to EXTENSION.

    To see the changes without committing them, use the --dry-run option.

    The --replace and --replace-all options will only replace valid suffixes (i.e., matching the expression
    \.[a-zA-Z0-9]+).

    The --append option will not add the new extension if it is already present.
    """
    extension = extension.strip()

    check_database_version(ctx, ctx_params(ctx)["root"], (db_path := root / "_metadata" / "files.db"))

    with FileDB(db_path) as database:
        log_file, log_stdout, _ = start_program(ctx, database, None, not dry_run, True, dry_run)

        with ExceptionManager(BaseException) as exception:
            for file in find_files(database, query):
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
                    HistoryEntry.command_history(
                        ctx, "skip", file.uuid, [str(old_name), str(new_name)], "No Changes"
                    ).log(INFO, log_stdout)
                    continue

                event = HistoryEntry.command_history(ctx, "edit", file.uuid, [str(old_name), str(new_name)], reason)

                if dry_run:
                    event.log(INFO, log_stdout)
                    continue

                file.root = root
                file.get_absolute_path().rename(file.get_absolute_path().with_name(new_name))
                file.relative_path = file.relative_path.with_name(new_name)

                try:
                    database.files.update(file, {"uuid": file.uuid})
                except SQLiteError:
                    file.get_absolute_path().rename(file.get_absolute_path().with_name(old_name))

                event.log(INFO, log_stdout)
                database.history.insert(event)

        end_program(ctx, database, exception, dry_run, log_file, log_stdout)
