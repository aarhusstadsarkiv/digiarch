from datetime import datetime
from logging import ERROR
from logging import INFO
from logging import Logger
from pathlib import Path
from sqlite3 import connect
from sqlite3 import Connection
from sqlite3 import DatabaseError
from sqlite3 import Row

from acacore.database import FilesDB
from acacore.models.event import Event
from acacore.models.file import MasterFile
from acacore.models.file import OriginalFile
from acacore.models.reference_files import ActionData
from acacore.models.reference_files import TActionType
from acacore.utils.click import ctx_params
from acacore.utils.click import end_program
from acacore.utils.click import start_program
from acacore.utils.helpers import ExceptionManager
from click import argument
from click import BadParameter
from click import command
from click import Context
from click import pass_context
from click import Path as ClickPath
from orjson import loads
from pydantic import BaseModel
from pydantic import UUID4

from digiarch.__version__ import __version__
from digiarch.common import AVID
from digiarch.common import get_avid


class OldFile(BaseModel):
    uuid: UUID4
    checksum: str
    relative_path: Path
    is_binary: bool
    size: int
    puid: str | None
    signature: str | None
    warning: list[str] | None = None
    action: TActionType | None
    action_data: ActionData
    parent: UUID4 | None = None
    processed: bool = False
    lock: bool = False
    original_path: Path
    processed_names: list[str]
    root: Path | None = None


class OldEvent(BaseModel):
    uuid: UUID4 | None
    time: datetime
    operation: str
    data: object | None
    reason: str | None


def import_original_file(avid: AVID, file: Row) -> tuple[OriginalFile, list[MasterFile], list[str]]:
    original_file: OriginalFile = OriginalFile.from_file(
        avid.dirs.original_documents.joinpath(file["relative_path"]),
        avid.path,
        uuid=file["uuid"],
        parent=file["parent"],
        processed=bool(file["processed"]),
        lock=bool(file["lock"]),
    )
    original_file.puid = file["puid"]
    original_file.signature = file["signature"]
    original_file.warning = loads(file["warning"]) if file["warning"] else None
    original_file.action = file["action"]
    original_file.action_data = loads(file["action_data"])
    original_file.original_path = avid.dirs.original_documents.joinpath(file["original_path"]).relative_to(avid.path)

    master_file_paths: list[Path] = [
        avid.dirs.master_documents.joinpath(file["relative_path"]).with_name(n) for n in loads(file["processed_names"])
    ]
    missing_master_files: list[str] = [f.name for f in master_file_paths if not f.is_file()]
    master_files: list[MasterFile] = [
        MasterFile.from_file(f, avid.path, original_file.uuid)
        for f in master_file_paths
        if f.name not in missing_master_files
    ]
    original_file.processed = original_file.processed and not missing_master_files

    return original_file, master_files, missing_master_files


def import_original_files(
    ctx: Context,
    avid: AVID,
    db: FilesDB,
    db_old: Connection,
    loggers: list[Logger],
) -> tuple[int, int]:
    original_files_cur = db_old.execute("select * from Files")
    original_files_cur.row_factory = Row

    imported_original_files: int = 0
    imported_master_files: int = 0

    for original_file_row in original_files_cur:
        original_file, master_files, missing_master_files = import_original_file(avid, original_file_row)
        db.original_files.insert(original_file)
        if master_files:
            db.master_files.insert(*master_files)
        Event.from_command(
            ctx,
            "imported",
            (original_file.uuid, "original"),
        ).log(INFO, *loggers, path=original_file.relative_path)
        for master_file in master_files:
            Event.from_command(
                ctx,
                "imported",
                (master_file.uuid, "master"),
            ).log(INFO, *loggers, path=master_file.relative_path)
        for missing_master_file in missing_master_files:
            Event.from_command(ctx, "missing-master", (original_file.uuid, "original")).log(
                ERROR,
                *loggers,
                name=missing_master_file,
            )
        imported_original_files += 1
        imported_master_files += len(master_files)

    return imported_original_files, imported_master_files


def import_log(db: FilesDB, db_old: Connection) -> int:
    log_cur = db_old.execute("select * from History")
    log_cur.row_factory = Row
    imported_events: int = 0

    for event_row in log_cur:
        event = Event(
            file_uuid=event_row["uuid"],
            file_type="original" if event_row["uuid"] else None,
            time=event_row["time"],
            operation=event_row["operation"],
            data=loads(event_row["data"]) if event_row["data"] else None,
            reason=event_row["reason"],
        )
        db.log.insert(event)
        imported_events += 1

    return imported_events


@command("import", no_args_is_help=True, short_help="Import files.db")
@argument("FILESDB", type=ClickPath(exists=True, dir_okay=False, readable=True, resolve_path=True), required=True)
@pass_context
def cmd_import(ctx: Context, filesdb: str):
    avid = get_avid(ctx)
    filesdb = Path(filesdb)
    if not filesdb.is_relative_to(avid.dirs.original_documents):
        raise BadParameter(
            f"{str(filesdb)!r} is not in {avid.dirs.original_documents.name}.",
            ctx,
            ctx_params(ctx)["filesdb"],
        )

    with FilesDB(avid.database_path) as db:
        _, log_stdout, _ = start_program(ctx, db, __version__, None, False)
        db.commit()

        with ExceptionManager(BaseException) as exception:
            if len(db.original_files):
                raise DatabaseError("Database already contains files.")

            db_old = connect(avid.dirs.original_documents.joinpath("_metadata", "files.db"))

            Event.from_command(ctx, "import.files:start").log(INFO, log_stdout)
            new_original_files, new_master_files = import_original_files(ctx, avid, db, db_old, [log_stdout])
            Event.from_command(ctx, "import.files:end").log(
                INFO,
                log_stdout,
                original_files=new_original_files,
                master_files=new_master_files,
            )

            Event.from_command(ctx, "import.log:start").log(INFO, log_stdout)
            new_events = import_log(db, db_old)
            Event.from_command(ctx, "import.log:end").log(INFO, log_stdout, events=new_events)

        if exception.exception:
            db.rollback()

        end_program(ctx, db, exception, False, log_stdout)
