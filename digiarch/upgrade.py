from datetime import datetime
from logging import INFO
from pathlib import Path
from shutil import copy2

from acacore.__version__ import __version__ as __acacore_version__
from acacore.database import FileDB
from acacore.database.upgrade import is_latest
from acacore.models.history import HistoryEntry
from acacore.utils.helpers import ExceptionManager
from click import BadParameter
from click import ClickException
from click import command
from click import Context
from click import option
from click import pass_context

from digiarch.common import argument_root
from digiarch.common import ctx_params
from digiarch.common import end_program
from digiarch.common import start_program


@command("upgrade", no_args_is_help=True, short_help="Upgrade the database.")
@argument_root(True)
@option(
    "--backup/--no-backup",
    is_flag=True,
    default=True,
    show_default=True,
    help="Backup current version.",
)
@pass_context
def command_upgrade(ctx: Context, root: Path, backup: bool):
    """
    Upgrade the files' database to the latest version of acacore.

    When using --backup, a copy of the current database version will be created in the same folder with the name
    "files-{version}.db". The copy will not be created if the database is already at the latest version.
    """
    start_time: datetime = datetime.now()

    with FileDB(root / "_metadata" / "files.db", check_version=False) as database:
        updated: bool = False
        log_file = log_stdout = None

        with ExceptionManager(BaseException, allow=[ClickException]) as exception:
            if not is_latest(database):
                if backup:
                    backup_path = database.path.with_stem(database.path.stem + f"-{database.metadata.select().version}")
                    if backup_path.exists() and database.path.stat().st_size != backup_path.stat().st_size:
                        raise BadParameter(
                            f"Backup file {backup_path.name} already exists.",
                            ctx,
                            ctx_params(ctx)["backup"],
                        )
                    copy2(database.path, backup_path)
                event = HistoryEntry.command_history(
                    ctx,
                    "update",
                    None,
                    [database.metadata.select().version, __acacore_version__],
                )
                database.upgrade()
                database.init()
                log_file, log_stdout, _ = start_program(ctx, database, start_time, True, True)
                database.history.insert(event)
                event.log(INFO, log_stdout)
                updated = True
            else:
                log_file, log_stdout, _ = start_program(ctx, database, start_time, False, True, True)
                HistoryEntry.command_history(ctx, "skip", reason="Database is already at the latest version").log(
                    INFO, log_stdout
                )

        end_program(ctx, database, exception, not updated, log_file, log_stdout)
