from logging import INFO
from typing import Literal

from acacore.database.table import Table
from acacore.models.event import Event
from acacore.models.file import MasterFile
from acacore.models.file import OriginalFile
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
from digiarch.common import ctx_params
from digiarch.common import get_avid
from digiarch.common import open_database
from digiarch.common import option_dry_run
from digiarch.query import argument_query
from digiarch.query import query_table
from digiarch.query import TQuery


@command("processed", no_args_is_help=True, short_help="Set files as processed.")
@argument("file_type", type=Choice(["original", "master"]), required=True)
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
    file_type: Literal["original", "master"],
    query: TQuery,
    reason: str,
    processed: bool,
    dry_run: bool,
):
    """
    Set files as processed.

    To set files as unprocessed, use the --unprocessed option.

    To see the changes without committing them, use the --dry-run option.

    For details on the QUERY argument, see the edit command.
    """
    avid = get_avid(ctx)

    with open_database(ctx, avid) as database:
        log_file, log_stdout, _ = start_program(ctx, database, __version__, None, True, True, dry_run)
        table: Table[OriginalFile | MasterFile]

        if file_type == "original":
            table = database.original_files
        elif file_type == "master":
            table = database.master_files
        else:
            raise BadParameter(f"invalid file type {file_type!r}", ctx, ctx_params(ctx)["file_type"])

        with ExceptionManager(BaseException) as exception:
            for file in query_table(table, query, [("lower(relative_path)", "asc")]):
                if file.processed == processed:
                    Event.from_command(ctx, "skip", (file.uuid, file_type), reason="No Changes").log(
                        INFO,
                        log_stdout,
                        path=file.relative_path,
                    )
                    continue
                event = Event.from_command(ctx, "edit", (file.uuid, file_type), [file.processed, processed], reason)
                if not dry_run:
                    file.processed = processed
                    table.update(file)
                    database.log.insert(event)
                event.log(INFO, log_stdout, show_args=["uuid", "data"], path=file.relative_path)

        end_program(ctx, database, exception, dry_run, log_file, log_stdout)
