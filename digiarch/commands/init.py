from logging import INFO
from pathlib import Path

from acacore.database import FilesDB
from acacore.database.upgrade import is_latest
from acacore.models.event import Event
from acacore.utils.click import end_program
from acacore.utils.click import start_program
from acacore.utils.helpers import ExceptionManager
from click import argument
from click import BadParameter
from click import command
from click import Context
from click import Parameter
from click import pass_context
from click import Path as ClickPath

from digiarch.__version__ import __version__
from digiarch.common import AVID


def root_callback(ctx: Context, param: Parameter, value: str):
    if not AVID.is_avid_dir(value):
        raise BadParameter("Not a valid AVID directory.", ctx, param)
    return Path(value)


@command("init", no_args_is_help=True, short_help="Initialize the database.")
@argument(
    "AVID_DIR",
    type=ClickPath(exists=True, file_okay=False, writable=True, readable=True, resolve_path=True),
    default=None,
    required=True,
    callback=root_callback,
)
@pass_context
def cmd_init(ctx: Context, avid_dir: Path):
    avid = AVID(avid_dir)
    avid.database_path.parent.mkdir(parents=True, exist_ok=True)

    with FilesDB(avid.database_path, check_initialisation=False, check_version=False) as db:
        _, log_stdout, event_start = start_program(ctx, db, __version__, None, False, True, True)
        initialized: bool = False

        with ExceptionManager(BaseException) as exception:
            if db.is_initialised():
                is_latest(db.connection, raise_on_difference=True)
                Event.from_command(ctx, "initialized", data=db.version()).log(INFO, log_stdout)
            else:
                db.init()
                db.log.insert(event_start)
                db.commit()
                Event.from_command(ctx, "initialized", data=db.version()).log(INFO, log_stdout)
                if avid.dirs.documents.exists() and not avid.dirs.original_documents.exists():
                    avid.dirs.documents.rename(avid.dirs.original_documents)
                    Event.from_command(ctx, "rename", data=["Documents", "OriginalDocuments"]).log(INFO, log_stdout)
                initialized = True

        end_program(ctx, db, exception, not initialized, log_stdout)
