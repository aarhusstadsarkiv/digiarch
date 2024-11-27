from acacore.utils.click import end_program
from acacore.utils.click import start_program
from acacore.utils.helpers import ExceptionManager
from click import argument
from click import command
from click import Context
from click import option
from click import pass_context

from digiarch.__version__ import __version__
from digiarch.common import get_avid
from digiarch.common import open_database
from digiarch.common import option_dry_run
from digiarch.query import argument_query
from digiarch.query import TQuery

from .common import edit_file_value


# noinspection DuplicatedCode
@command("lock", no_args_is_help=True, short_help="Lock files.")
@argument_query(True, "uuid", ["uuid", "checksum", "puid", "relative_path", "action", "warning", "processed", "lock"])
@argument("reason", nargs=1, type=str, required=True)
@option("--lock/--unlock", is_flag=True, default=True, show_default=True, help="Lock or unlock files.")
@option_dry_run()
@pass_context
def cmd_lock_original(
    ctx: Context,
    query: TQuery,
    reason: str,
    lock: bool,
    dry_run: bool,
):
    """
    Lock original files from being edited by reidentify.

    To unlock files, use the --unlock option.

    To see the changes without committing them, use the --dry-run option.

    For details on the QUERY argument, see the edit command.
    """
    avid = get_avid(ctx)

    with open_database(ctx, avid) as database:
        log_file, log_stdout, _ = start_program(ctx, database, __version__, None, True, True, dry_run)

        with ExceptionManager(BaseException) as exception:
            edit_file_value(
                ctx,
                database,
                database.original_files,
                query,
                reason,
                "original",
                "lock",
                lock,
                dry_run,
                log_stdout,
            )

        end_program(ctx, database, exception, dry_run, log_file, log_stdout)
