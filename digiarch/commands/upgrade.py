from logging import INFO
from shutil import copy2

from acacore.__version__ import __version__ as __acacore_version__
from acacore.database import FilesDB
from acacore.database.upgrade import is_latest
from acacore.models.event import Event
from acacore.utils.click import end_program
from acacore.utils.click import start_program
from acacore.utils.helpers import ExceptionManager
from click import BadParameter
from click import ClickException
from click import command
from click import Context
from click import option
from click import pass_context

from digiarch.__version__ import __version__
from digiarch.common import ctx_params
from digiarch.common import get_avid


@command("upgrade", short_help="Upgrade the database.")
@option(
    "--backup/--no-backup",
    is_flag=True,
    default=True,
    show_default=True,
    help="Backup current version.",
)
@pass_context
def cmd_upgrade(ctx: Context, backup: bool):
    """
    Upgrade the files' database to the latest version of acacore.

    When using --backup, a copy of the current database version will be created in the same folder with the name
    "avid-{version}.db". The copy will not be created if the database is already at the latest version.
    """
    avid = get_avid(ctx)
    with FilesDB(avid.database_path, check_initialisation=True, check_version=False) as database:
        log_file, log_stdout, start_event = start_program(ctx, database, __version__, None, True, True, True)
        updated: bool = False

        with ExceptionManager(BaseException, allow=[ClickException]) as exception:
            if not is_latest(database.connection):
                if backup:
                    backup_path = database.path.with_stem(database.path.stem + f"-{database.metadata.get().version}")
                    if backup_path.exists() and database.path.stat().st_size != backup_path.stat().st_size:
                        raise BadParameter(
                            f"Backup file {backup_path.name} already exists.",
                            ctx,
                            ctx_params(ctx)["backup"],
                        )
                    copy2(database.path, backup_path)
                event = Event.command_history(
                    ctx,
                    "update",
                    None,
                    [database.metadata.get().version, __acacore_version__],
                )
                database.upgrade()
                database.init()
                database.log.insert(start_event)
                database.log.insert(event)
                event.log(INFO, log_stdout)
                updated = True
            else:
                Event.command_history(
                    ctx,
                    "skip",
                    reason="Database is already at the latest version",
                ).log(INFO, log_stdout)

        end_program(ctx, database, exception, not updated, log_file, log_stdout)
