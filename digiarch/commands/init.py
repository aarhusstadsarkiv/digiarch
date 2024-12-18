from logging import ERROR
from logging import INFO
from logging import Logger
from logging import WARNING
from os import PathLike
from pathlib import Path
from pathlib import PureWindowsPath
from sqlite3 import connect
from sqlite3 import Connection
from sqlite3 import Row
from typing import Literal

from acacore.database import FilesDB
from acacore.database.upgrade import is_latest
from acacore.models.event import Event
from acacore.models.file import MasterFile
from acacore.models.file import OriginalFile
from acacore.utils.click import ctx_params
from acacore.utils.click import end_program
from acacore.utils.click import start_program
from acacore.utils.helpers import ExceptionManager
from click import argument
from click import BadParameter
from click import command
from click import Context
from click import option
from click import Parameter
from click import pass_context
from click import Path as ClickPath
from orjson import loads

from digiarch.__version__ import __version__
from digiarch.common import AVID


def root_callback(ctx: Context, param: Parameter, value: str) -> AVID:
    if not AVID.is_avid_dir(value):
        raise BadParameter(f"{value!r} is not a valid AVID directory.", ctx, param)
    return AVID(value)


def import_acacore_original_file(avid: AVID, file: Row) -> tuple[OriginalFile, list[MasterFile], list[str]]:
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


def import_acacore_files(
    ctx: Context,
    avid: AVID,
    db: FilesDB,
    db_old: Connection,
    *loggers: Logger,
) -> tuple[int, int, int]:
    original_files_cur = db_old.execute("select * from Files")
    original_files_cur.row_factory = Row

    total_imported_original_files: int = 0
    total_imported_master_files: int = 0
    total_missing_master_files: int = 0

    for original_file_row in original_files_cur:
        original_file, master_files, missing_master_files = import_acacore_original_file(avid, original_file_row)
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
        total_imported_original_files += 1
        total_imported_master_files += len(master_files)
        total_missing_master_files += len(missing_master_files)

    return total_imported_original_files, total_imported_master_files, total_missing_master_files


def import_files(ctx: Context, avid: AVID, db: FilesDB, db_old: Connection, *loggers: Logger) -> tuple[int, int, int]:
    # noinspection SqlResolve
    paths_cursor = db_old.execute("select path from Files")

    total_imported_original_files: int = 0
    total_imported_master_files: int = 0

    path_str: str
    for [path_str] in paths_cursor:
        path = Path(PureWindowsPath(path_str)) if "\\" in path_str else Path(path_str)
        if "originaldocuments" not in (path_parts := [p.lower() for p in path.parts]):
            Event.from_command(ctx, "skip").log(
                WARNING,
                *loggers,
                path=path,
                reason="File is not in OriginalDocuments",
            )
            continue
        path = avid.dirs.original_documents.joinpath(*path_parts[path_parts.index("originaldocuments") + 1 :])
        if not path.is_file():
            Event.from_command(ctx, "skip").log(
                WARNING,
                *loggers,
                path=path.relative_to(avid.path),
                reason="File not found",
            )
            continue

        original_file = OriginalFile.from_file(path, avid.path)
        db.original_files.insert(original_file)
        Event.from_command(ctx, "imported", (original_file.uuid, "original")).log(
            INFO,
            *loggers,
            path=original_file.relative_path,
        )
        total_imported_original_files += 1

        master_files_dir: Path = avid.dirs.master_documents.joinpath(
            path.parent.relative_to(avid.dirs.original_documents)
        )
        master_files: list[MasterFile] = [
            MasterFile.from_file(f, avid.path, original_file.uuid)
            for f in master_files_dir.iterdir()
            if f.is_file() and f.stem == path.stem
        ]
        db.master_files.insert(*master_files)
        for master_file in master_files:
            Event.from_command(ctx, "imported", (master_file.uuid, "master")).log(
                INFO,
                *loggers,
                path=master_file.relative_path,
            )
        total_imported_master_files += len(master_files)

    return total_imported_original_files, total_imported_master_files, 0


def import_db(
    ctx: Context,
    avid: AVID,
    db: FilesDB,
    import_db_path: str | PathLike,
    import_mode: Literal["acacore", "files"],
    *loggers: Logger,
):
    db_old = connect(import_db_path)
    new_original_files: int = 0
    new_master_files: int = 0
    missing_master_files: int = 0

    Event.from_command(ctx, "import:start").log(INFO, *loggers, type=import_mode)

    if import_mode == "acacore":
        new_original_files, new_master_files, missing_master_files = import_acacore_files(
            ctx,
            avid,
            db,
            db_old,
            *loggers,
        )
    elif import_mode == "files":
        new_original_files, new_master_files, missing_master_files = import_files(ctx, avid, db, db_old, *loggers)

    Event.from_command(ctx, "import:end").log(
        INFO,
        *loggers,
        original_files=new_original_files,
        master_files=new_master_files,
        missing_master_files=missing_master_files,
    )

    db.log.insert(
        Event.from_command(
            ctx,
            "import",
            None,
            {
                "mode": import_mode,
                "original_files": new_original_files,
                "master_files": new_master_files,
                "missing_master_files": missing_master_files,
            },
        )
    )
    db.commit()


def check_import_db(
    ctx: Context,
    import_db_path: str | PathLike,
    import_param_name: str,
) -> Literal["acacore", "files"]:
    db_old: Connection | None = None

    try:
        db_old = connect(import_db_path)

        tables: list[str] = [t.lower() for [t] in db_old.execute("select name from sqlite_master where type = 'table'")]
        if "files" not in tables:
            raise BadParameter("Invalid database schema.", ctx, ctx_params(ctx)[import_param_name])
        if "metadata" not in tables or (
            {c.lower() for [_, c, *_] in db_old.execute("pragma table_info(metadata)")} != {"key", "value"}
        ):
            return "files"

        version = db_old.execute("select value from Metadata where key = 'version'").fetchone()
        if not version:
            raise BadParameter("No version information.", ctx, ctx_params(ctx)[import_param_name])
        if version[0] != "3.3.3":
            raise BadParameter(f"Invalid version {version[0]}, must be 3.3.3.", ctx, ctx_params(ctx)[import_param_name])

        return "acacore"
    finally:
        if db_old:
            db_old.close()


@command("init", no_args_is_help=True, short_help="Initialize the database.")
@argument(
    "avid",
    metavar="AVID_DIR",
    type=ClickPath(exists=True, file_okay=False, writable=True, readable=True, resolve_path=True),
    default=None,
    required=True,
    callback=root_callback,
)
@option(
    "--import",
    "import_db_path",
    type=ClickPath(exists=True, dir_okay=False, readable=True, resolve_path=True),
    default=None,
    required=False,
    help="Import an existing files.db",
)
@pass_context
def cmd_init(ctx: Context, avid: AVID, import_db_path: str | None):
    """
    Initialize the AVID database in a directory (AVID_DIR).

    The directory is checked to make sure it has the structure expected of an AVID archive.

    The --import option allows to import original and master files from a files.db database generated by version
    v4.1.12 of digiarch (acacore v3.3.3). A pre-acacore version of the database can also be used if it contains a
    'Files' table with a 'path' column, but some master files may be missing.
    """
    avid.database_path.parent.mkdir(parents=True, exist_ok=True)
    import_mode: Literal["acacore", "files"] | None = None

    if import_db_path:
        import_mode = check_import_db(ctx, import_db_path, "import_db_path")

    with FilesDB(avid.database_path, check_initialisation=False, check_version=False) as db:
        _, log_stdout, event_start = start_program(ctx, db, __version__, None, False, True, True)
        initialized: bool = False

        with ExceptionManager(BaseException) as exception:
            if db.is_initialised():
                is_latest(db.connection, raise_on_difference=True)
                Event.from_command(ctx, "initialized").log(INFO, log_stdout, version=db.version())
            else:
                db.init()
                db.log.insert(event_start)
                db.commit()

                if avid.dirs.documents.exists() and not avid.dirs.original_documents.exists():
                    avid.dirs.documents.rename(avid.dirs.original_documents)
                    event = Event.from_command(ctx, "rename", data=["Documents", "OriginalDocuments"])
                    db.log.insert(event)
                    event.log(INFO, log_stdout)

                initialized = True
                event = Event.from_command(ctx, "initialized", data=(v := db.version()))
                db.log.insert(event)
                event.log(INFO, log_stdout, show_args=False, version=v)

            if initialized and import_db_path and import_mode is not None:
                import_db(ctx, avid, db, import_db_path, import_mode, log_stdout)

        end_program(ctx, db, exception, not initialized, log_stdout)

    if initialized and exception.exception:
        avid.database_path.unlink(missing_ok=True)
