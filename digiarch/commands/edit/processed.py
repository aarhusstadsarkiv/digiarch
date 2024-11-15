from logging import INFO
from pathlib import Path

from acacore.database import FileDB
from acacore.models.history import HistoryEntry
from acacore.utils.click import check_database_version
from acacore.utils.click import end_program
from acacore.utils.click import start_program
from acacore.utils.helpers import ExceptionManager
from click import argument
from click import command
from click import Context
from click import option
from click import pass_context

from digiarch.__version__ import __version__
from digiarch.common import argument_root
from digiarch.common import ctx_params
from digiarch.common import option_dry_run

from .common import argument_query
from .common import find_files
from .common import TQuery


@command("processed", no_args_is_help=True, short_help="Set files as processed.")
@argument_root(True)
@argument_query(True, "uuid", ["uuid", "checksum", "puid", "relative_path", "action", "warning", "processed", "lock"])
@argument("reason", nargs=1, type=str, required=True)
@option(
    "--processed/--unprocessed",
    is_flag=True,
    default=True,
    show_default=True,
    help="Set files as processed or unprocessed.",
)
@option_dry_run()
@pass_context
def command_processed(
    ctx: Context,
    root: Path,
    reason: str,
    query: TQuery,
    processed: bool,
    dry_run: bool,
):
    """
    Set files as processed.

    To set files as unprocessed, use the --unprocessed option.

    To see the changes without committing them, use the --dry-run option.

    For details on the QUERY argument, see the edit command.
    """
    check_database_version(ctx, ctx_params(ctx)["root"], (db_path := root / "_metadata" / "files.db"))

    with FileDB(db_path) as database:
        log_file, log_stdout, _ = start_program(ctx, database, __version__, None, True, True, dry_run)

        with ExceptionManager(BaseException) as exception:
            for file in find_files(database, query):
                if file.processed == processed:
                    HistoryEntry.command_history(ctx, "skip", file.uuid, None, "No Changes").log(INFO, log_stdout)
                    continue
                event = HistoryEntry.command_history(ctx, "edit", file.uuid, [file.processed, processed], reason)
                if not dry_run:
                    file.processed = processed
                    database.files.update(file)
                    database.history.insert(event)
                event.log(INFO, log_stdout)

        end_program(ctx, database, exception, dry_run, log_file, log_stdout)
