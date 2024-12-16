from acacore.utils.click import end_program
from acacore.utils.click import start_program
from acacore.utils.helpers import ExceptionManager
from click import argument
from click import command
from click import Context
from click import option
from click import pass_context

from digiarch.__version__ import __version__
from digiarch.common import CommandWithRollback
from digiarch.common import get_avid
from digiarch.common import open_database
from digiarch.common import option_dry_run
from digiarch.common import rollback
from digiarch.query import argument_query
from digiarch.query import TQuery

from .common import edit_file_value
from .common import rollback_file_value


# noinspection DuplicatedCode
@rollback("edit", rollback_file_value("lock"))
@command("processed", no_args_is_help=True, short_help="Set original files as processed.", cls=CommandWithRollback)
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
def cmd_processed_original(ctx: Context, query: TQuery, reason: str, processed: bool, dry_run: bool):
    """
    Set original files matching the QUERY argument as processed.

    To set files as unprocessed, use the --unprocessed option.

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
                "processed",
                processed,
                dry_run,
                log_stdout,
            )

        end_program(ctx, database, exception, dry_run, log_file, log_stdout)


@rollback("edit", rollback_file_value("lock"))
@command("processed", no_args_is_help=True, short_help="Set master files as processed.", cls=CommandWithRollback)
@argument_query(True, "uuid", ["uuid", "checksum", "puid", "relative_path", "action", "warning", "processed"])
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
def cmd_processed_master(ctx: Context, query: TQuery, reason: str, processed: bool, dry_run: bool):
    """
    Set master files matching the QUERY argument as processed.

    To set files as unprocessed, use the --unprocessed option.

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
                database.master_files,
                query,
                reason,
                "master",
                "processed",
                processed,
                dry_run,
                log_stdout,
            )

        end_program(ctx, database, exception, dry_run, log_file, log_stdout)
