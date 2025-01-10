from acacore.utils.click import end_program
from acacore.utils.click import start_program
from acacore.utils.helpers import ExceptionManager
from click import argument
from click import command
from click import Context
from click import option
from click import pass_context

from digiarch.__version__ import __version__
from digiarch.commands.edit.common import edit_file_value
from digiarch.commands.edit.common import rollback_file_value
from digiarch.common import CommandWithRollback
from digiarch.common import get_avid
from digiarch.common import open_database
from digiarch.common import option_dry_run
from digiarch.common import rollback
from digiarch.query import argument_query
from digiarch.query import TQuery


@rollback("edit", rollback_file_value("puid"))
@command("puid", no_args_is_help=True, short_help="Change PUID.", cls=CommandWithRollback)
@argument("puid", nargs=1, type=str, required=True)
@argument_query(True, "uuid", ["uuid", "checksum", "puid", "relative_path", "action", "warning", "processed", "lock"])
@argument("reason", nargs=1, type=str, required=True)
@option("--lock", is_flag=True, default=False, help="Lock the edited files.")
@option_dry_run()
@pass_context
def cmd_puid_original(
    ctx: Context,
    puid: str,
    query: TQuery,
    reason: str,
    lock: bool,
    dry_run: bool,
):
    """
    Change PUID of original files.

    To lock the files after editing them, use the --lock option.

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
                "puid",
                puid,
                dry_run,
                log_stdout,
                lock=lock,
            )

        end_program(ctx, database, exception, dry_run, log_file, log_stdout)


@rollback("edit", rollback_file_value("puid"))
@command("puid", no_args_is_help=True, short_help="Change PUID.", cls=CommandWithRollback)
@argument("puid", nargs=1, type=str, required=True)
@argument_query(True, "uuid", ["uuid", "checksum", "puid", "relative_path", "warning", "processed"])
@argument("reason", nargs=1, type=str, required=True)
@option_dry_run()
@pass_context
def cmd_puid_master(
    ctx: Context,
    puid: str,
    query: TQuery,
    reason: str,
    dry_run: bool,
):
    """
    Change PUID of master files.

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
                "puid",
                puid,
                dry_run,
                log_stdout,
            )

        end_program(ctx, database, exception, dry_run, log_file, log_stdout)
