from logging import INFO
from pathlib import Path

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
from digiarch.common import start_program

from .common import argument_query
from .common import find_files
from .common import TQuery


def remove_empty_dir(root: Path, path: Path):
    if path == root:
        return
    elif not path.is_relative_to(root):
        return
    elif not path.is_dir():
        return
    elif next(path.iterdir(), None):
        return

    path.rmdir()

    return remove_empty_dir(root, path.parent)


@command("remove", no_args_is_help=True, short_help="Remove files.")
@argument_root(True)
@argument_query(True, "uuid", ["uuid", "checksum", "puid", "relative_path", "action", "warning", "processed", "lock"])
@argument("reason", nargs=1, type=str, required=True)
@option("--delete", is_flag=True, default=False, help="Remove selected files from the disk.")
@option_dry_run()
@pass_context
def command_remove(
    ctx: Context,
    root: Path,
    reason: str,
    query: TQuery,
    delete: bool,
    dry_run: bool,
):
    """
    Remove one or more files in the files' database for the ROOT folder to EXTENSION.

    Using the --delete option removes the files from the disk.

    To see the changes without committing them, use the --dry-run option.

    For details on the QUERY argument, see the edit command.
    """
    check_database_version(ctx, ctx_params(ctx)["root"], (db_path := root / "_metadata" / "files.db"))

    with FileDB(db_path) as database:
        log_file, log_stdout, _ = start_program(ctx, database, None, True, True, dry_run)

        with ExceptionManager(BaseException) as exception:
            for file in find_files(database, query):
                event = HistoryEntry.command_history(
                    ctx, "delete" if delete else "remove", file.uuid, file.model_dump(mode="json"), reason
                )
                if not dry_run:
                    database.execute(f"delete from {database.files.name} where uuid = ?", [str(file.uuid)])
                    database.history.insert(event)
                    if delete:
                        file.get_absolute_path(root).unlink(missing_ok=True)
                        remove_empty_dir(root, file.get_absolute_path(root).parent)
                event.log(INFO, log_stdout)

        end_program(ctx, database, exception, dry_run, log_file, log_stdout)
