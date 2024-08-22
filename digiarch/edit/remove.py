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

from .common import argument_ids
from .common import find_files


@command("remove", no_args_is_help=True, short_help="Remove files.")
@argument_root(True)
@argument_ids(True)
@argument("reason", nargs=1, type=str, required=True)
@option("--delete", is_flag=True, default=False, help="Remove selected files from the disk.")
@option_dry_run()
@pass_context
def command_remove(
    ctx: Context,
    root: Path,
    reason: str,
    ids: tuple[str, ...],
    id_type: str,
    id_files: bool,
    delete: bool,
    dry_run: bool,
):
    """
    Remove one or more files in the files' database for the ROOT folder to EXTENSION.

    Using the --delete option removes the files from the disk.

    To see the changes without committing them, use the --dry-run option.

    For details on the ID arguments, see the edit command.
    """
    check_database_version(ctx, ctx_params(ctx)["root"], (db_path := root / "_metadata" / "files.db"))

    with FileDB(db_path) as database:
        log_file, log_stdout, _ = start_program(ctx, database, None, True, True, dry_run)

        with ExceptionManager(BaseException) as exception:
            for file in find_files(database, ids, id_type, id_files):
                event = HistoryEntry.command_history(
                    ctx, "delete" if delete else "remove", file.uuid, file.model_dump(mode="json"), reason
                )
                if not dry_run:
                    database.execute(f"delete from {database.files.name} where uuid = ?", [str(file.uuid)])
                    database.history.insert(event)
                    if delete:
                        file.get_absolute_path(root).unlink(missing_ok=True)
                event.log(INFO, log_stdout)

        end_program(ctx, database, exception, dry_run, log_file, log_stdout)
