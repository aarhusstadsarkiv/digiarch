from datetime import datetime
from functools import reduce
from itertools import islice
from json import loads
from logging import ERROR
from logging import INFO
from logging import Logger
from os import environ
from pathlib import Path
from re import IGNORECASE
from re import match
from re import RegexFlag
from shutil import copy2
from sqlite3 import DatabaseError
from sqlite3 import Error as SQLiteError
from sys import stdout
from traceback import format_tb
from typing import Any
from typing import Callable
from typing import Generator
from typing import get_args as get_type_args
from typing import Sequence
from typing import TypeVar
from uuid import UUID
from uuid import uuid4

import yaml
from acacore.__version__ import __version__ as __acacore_version__
from acacore.database import FileDB
from acacore.database.upgrade import is_latest
from acacore.models.file import File
from acacore.models.history import HistoryEntry
from acacore.models.reference_files import Action
from acacore.models.reference_files import ActionData
from acacore.models.reference_files import ConvertAction
from acacore.models.reference_files import CustomSignature
from acacore.models.reference_files import ExtractAction
from acacore.models.reference_files import IgnoreAction
from acacore.models.reference_files import ManualAction
from acacore.models.reference_files import ReIdentifyAction
from acacore.models.reference_files import RenameAction
from acacore.models.reference_files import TActionType
from acacore.models.reference_files import TemplateAction
from acacore.reference_files import get_actions
from acacore.reference_files import get_custom_signatures
from acacore.siegfried import Siegfried
from acacore.siegfried.siegfried import SiegfriedFile
from acacore.siegfried.siegfried import TSignaturesProvider
from acacore.utils.functions import find_files
from acacore.utils.helpers import ExceptionManager
from acacore.utils.log import setup_logger
from click import argument
from click import BadParameter
from click import Choice
from click import Context
from click import DateTime
from click import group
from click import IntRange
from click import option
from click import Parameter
from click import pass_context
from click import Path as ClickPath
from click import version_option
from pydantic import TypeAdapter

from .__version__ import __version__

FC = TypeVar("FC", bound=Callable[..., Any])


def argument_ids(required: bool = True) -> Callable[[FC], FC]:
    def inner(callback: FC) -> FC:
        decorators: list[Callable[[FC], FC]] = [
            argument(
                "ids",
                metavar="ID...",
                nargs=-1,
                type=str,
                required=required,
                callback=lambda _c, _p, v: tuple(sorted(set(v), key=v.index)),
            ),
            option(
                "--uuid",
                "id_type",
                flag_value="uuid",
                default=True,
                help="Use UUID's as identifiers.  [default]",
            ),
            option(
                "--puid",
                "id_type",
                flag_value="puid",
                help="Use PUID's as identifiers.",
            ),
            option(
                "--path",
                "id_type",
                flag_value="relative_path",
                help="Use relative paths as identifiers.",
            ),
            option(
                "--path-like",
                "id_type",
                flag_value="relative_path-like",
                help="Use relative paths as identifiers, match with LIKE.",
            ),
            option(
                "--checksum",
                "id_type",
                flag_value="checksum",
                help="Use checksums as identifiers.",
            ),
            option(
                "--warning",
                "id_type",
                flag_value="warnings",
                help="Use warnings as identifiers.",
            ),
            option(
                "--id-files",
                is_flag=True,
                default=False,
                help="Interpret IDs as files from which to read the IDs.",
            ),
        ]
        for decorator in reversed(decorators):
            callback = decorator(callback)
        return callback

    return inner


def handle_rename(file: File, action: RenameAction) -> tuple[Path, Path] | tuple[None, None]:
    old_path: Path = file.get_absolute_path()

    if action.on_extension_mismatch and (not file.warning or "extension mismatch" not in file.warning):
        return None, None

    new_suffixes: list[str] = [action.extension] if not action.append else [*old_path.suffixes, action.extension]
    new_path: Path = old_path.with_suffix("".join(new_suffixes))
    if old_path == new_path:
        return None, None
    old_path.rename(new_path)
    return old_path, new_path


def handle_start(ctx: Context, database: FileDB, *loggers: Logger, time: datetime | None = None):
    program_start: HistoryEntry = HistoryEntry.command_history(
        ctx,
        "start",
        data={"version": __version__},
        add_params_to_data=True,
        time=time,
    )

    database.history.insert(program_start)
    program_start.log(INFO, *loggers, show_args=False)


def handle_end(ctx: Context, database: FileDB, exception: ExceptionManager, *loggers: Logger, commit: bool = True):
    program_end: HistoryEntry = HistoryEntry.command_history(
        ctx,
        "end",
        data=repr(exception.exception) if exception.exception else None,
        reason="".join(format_tb(exception.traceback)) if exception.traceback else None,
    )

    program_end.log(ERROR if exception.exception else INFO, *loggers)

    if database.is_open and commit:
        database.history.insert(program_end)
        database.commit()


def identify_file(
    ctx: Context,
    root: Path,
    path: Path,
    database: FileDB,
    siegfried: Siegfried,
    siegfried_result: SiegfriedFile | None,
    actions: dict[str, Action],
    custom_signatures: list[CustomSignature],
    *,
    update: bool = False,
) -> tuple[File | None, list[HistoryEntry]]:
    uuid: UUID
    existing_file: File | None = database.files.select(
        where="relative_path = ?",
        limit=1,
        parameters=[str(path.relative_to(root))],
    ).fetchone()

    if existing_file and update:
        uuid = existing_file.uuid
    elif existing_file:
        return None, []
    else:
        uuid = uuid4()
        update = False

    file_history: list[HistoryEntry] = []

    with ExceptionManager(
        Exception,
        allow=[OSError, IOError],
    ) as identify_error:
        file = File.from_file(path, root, siegfried_result or siegfried, actions, custom_signatures, uuid=uuid)

    if identify_error.exception:
        file = File.from_file(path, root, siegfried_result or siegfried)
        file.action = "manual"
        file.action_data = ActionData(
            manual=ManualAction(
                reason=identify_error.exception.__class__.__name__,
                process="Identify and fix error.",
            ),
        )
        file_history.append(
            HistoryEntry.command_history(
                ctx,
                "file:identify:error",
                file.uuid,
                repr(identify_error.exception),
                "".join(format_tb(identify_error.traceback)) if identify_error.traceback else None,
            ),
        )

    if file.action_data and file.action_data.rename:
        old_path, new_path = handle_rename(file, file.action_data.rename)
        if new_path:
            file = File.from_file(new_path, root, siegfried, actions, custom_signatures, uuid=file.uuid)
            file_history.append(
                HistoryEntry.command_history(
                    ctx,
                    "file:action:rename",
                    file.uuid,
                    [old_path.relative_to(root), new_path.relative_to(root)],
                ),
            )

    if update:
        database.files.update(file, {"uuid": file.uuid})
    else:
        database.files.insert(file, exist_ok=True)

    return file, file_history


def regex_callback(pattern: str, flags: int | RegexFlag = 0) -> Callable[[Context, Parameter, str], str]:
    def _callback(ctx: Context, param: Parameter, value: str | Sequence[str]):
        if isinstance(value, (list, tuple)):
            if any(not match(pattern, v, flags) for v in value):
                raise BadParameter(f"{value!r} does not match pattern {pattern}", ctx, param)
        elif not match(pattern, value, flags):
            raise BadParameter(f"{value!r} does not match pattern {pattern}", ctx, param)
        return value

    return _callback


@group("digiarch", no_args_is_help=True)
@version_option(__version__)
def app():
    """Generate and operate on the files' database used by other Aarhus Stadsarkiv tools."""


@app.command("identify", no_args_is_help=True, short_help="Generate a files' database for a folder.")
@argument("root", type=ClickPath(exists=True, file_okay=False, writable=True, resolve_path=True))
@option(
    "--siegfried-path",
    type=ClickPath(dir_okay=False, resolve_path=True),
    envvar="SIEGFRIED_PATH",
    default=None,
    show_envvar=True,
    help="The path to the Siegfried executable.",
)
@option(
    "--siegfried-home",
    type=ClickPath(file_okay=False, resolve_path=True),
    envvar="SIEGFRIED_HOME",
    default=None,
    show_envvar=True,
    help="The path to the Siegfried home folder.",
)
@option(
    "--siegfried-signature",
    type=Choice(get_type_args(TSignaturesProvider)),
    default="pronom",
    show_default=True,
    help="The signature file to use with Siegfried.",
)
@option(
    "--update-siegfried-signature/--no-update-siegfried-signature",
    is_flag=True,
    default=False,
    show_default=True,
    help="Control whether Siegfried should update its signature.",
)
@option(
    "--actions",
    "actions_file",
    type=ClickPath(exists=True, dir_okay=False, file_okay=True, resolve_path=True),
    envvar="DIGIARCH_ACTIONS",
    show_envvar=True,
    default=None,
    help="Path to a YAML file containing file format actions.",
)
@option(
    "--custom-signatures",
    "custom_signatures_file",
    type=ClickPath(exists=True, dir_okay=False, file_okay=True, resolve_path=True),
    envvar="DIGIARCH_CUSTOM_SIGNATURES",
    show_envvar=True,
    default=None,
    help="Path to a YAML file containing custom signature specifications.",
)
@option("--batch-size", type=IntRange(1), default=100)
@pass_context
def app_identify(
    ctx: Context,
    root: str | Path,
    siegfried_path: str | None,
    siegfried_home: str | None,
    siegfried_signature: TSignaturesProvider,
    update_siegfried_signature: bool,
    actions_file: str | None,
    custom_signatures_file: str | None,
    batch_size: int,
    *,
    update_where: list[tuple[str, Sequence[str]]] | None = None,
):
    """
    Process a folder (ROOT) recursively and populate a files' database.

    Each file is identified with Siegfried and an action is assigned to it.
    Files that need re-identification, renaming, or ignoring are processed accordingly.

    Files that are already in the database are not processed.
    """
    root = Path(root)
    siegfried = Siegfried(
        siegfried_path or Path(environ["GOPATH"], "bin", "sf"),
        f"{siegfried_signature}.sig",
        siegfried_home,
    )
    if update_siegfried_signature:
        siegfried.update(siegfried_signature)

    actions: dict[str, Action]
    custom_signatures: list[CustomSignature]

    if actions_file:
        with Path(actions_file).open() as fh:
            actions = TypeAdapter(dict[str, Action]).validate_python(yaml.load(fh, yaml.Loader))
    else:
        actions = get_actions()

    if custom_signatures_file:
        with Path(custom_signatures_file).open() as fh:
            custom_signatures = TypeAdapter(list[CustomSignature]).validate_python(yaml.load(fh, yaml.Loader))
    else:
        custom_signatures = get_custom_signatures()

    database_path: Path = Path(root) / "_metadata" / "files.db"
    database_path.parent.mkdir(exist_ok=True)

    program_name: str = ctx.find_root().command.name
    logger: Logger = setup_logger(program_name, files=[database_path.parent / f"{program_name}.log"], streams=[stdout])
    logger_stdout: Logger = setup_logger(program_name + "_std_out", streams=[stdout])

    with FileDB(database_path) as database:
        database.init()
        handle_start(ctx, database, logger)

        with ExceptionManager(BaseException) as exception:
            files: Generator[Path, None, None]

            if update_where:
                files = (
                    f.get_absolute_path(root)
                    for w, p in update_where
                    for f in database.files.select(where=w, parameters=p)
                )
            else:
                files = find_files(root, exclude=[database_path.parent])

            while batch := list(islice(files, batch_size)):
                for path, result in siegfried.identify(*batch).files_dict.items():
                    file, file_history = identify_file(
                        ctx,
                        root,
                        path,
                        database,
                        siegfried,
                        result,
                        actions,
                        custom_signatures,
                        update=update_where is not None,
                    )

                    if file:
                        HistoryEntry.command_history(
                            ctx,
                            ":file:" + ("update" if update_where else "new"),
                            file.uuid,
                        ).log(
                            INFO,
                            logger_stdout,
                            path=file.relative_path,
                            puid=file.puid,
                            action=file.action,
                        )

                    for event in file_history:
                        event.log(INFO, logger)
                        database.history.insert(event)

        handle_end(ctx, database, exception, logger)


@app.command("reidentify", no_args_is_help=True, short_help="Reidentify files.")
@argument("root", type=ClickPath(exists=True, file_okay=False, writable=True, resolve_path=True))
@argument_ids(False)
@option(
    "--siegfried-path",
    type=ClickPath(dir_okay=False, resolve_path=True),
    envvar="SIEGFRIED_PATH",
    default=None,
    show_envvar=True,
    help="The path to the Siegfried executable.",
)
@option(
    "--siegfried-home",
    type=ClickPath(file_okay=False, resolve_path=True),
    envvar="SIEGFRIED_HOME",
    default=None,
    show_envvar=True,
    help="The path to the Siegfried home folder.",
)
@option(
    "--siegfried-signature",
    type=Choice(get_type_args(TSignaturesProvider)),
    default="pronom",
    show_default=True,
    help="The signature file to use with Siegfried.",
)
@option(
    "--update-siegfried-signature/--no-update-siegfried-signature",
    is_flag=True,
    default=False,
    show_default=True,
    help="Control whether Siegfried should update its signature.",
)
@option(
    "--actions",
    "actions_file",
    type=ClickPath(exists=True, dir_okay=False, file_okay=True, resolve_path=True),
    envvar="DIGIARCH_ACTIONS",
    show_envvar=True,
    default=None,
    help="Path to a YAML file containing file format actions.",
)
@option(
    "--custom-signatures",
    "custom_signatures_file",
    type=ClickPath(exists=True, dir_okay=False, file_okay=True, resolve_path=True),
    envvar="DIGIARCH_CUSTOM_SIGNATURES",
    show_envvar=True,
    default=None,
    help="Path to a YAML file containing custom signature specifications.",
)
@option("--batch-size", type=IntRange(1), default=100)
@pass_context
def app_reidentify(
    _ctx: Context,
    root: str | Path,
    ids: tuple[str],
    id_type: str,
    id_files: bool,
    siegfried_path: str | None,
    siegfried_home: str | None,
    siegfried_signature: TSignaturesProvider,
    update_siegfried_signature: bool,
    actions_file: str | None,
    custom_signatures_file: str | None,
    batch_size: int,
):
    """
    Re-indentify specific files.

    Each file is re-identified with Siegfried and an action is assigned to it.
    Files that need re-identification with custom signatures, renaming, or ignoring are processed accordingly.

    The ID arguments are interpreted as a list of UUID's by default. The behaviour can be changed with the
    --puid, --path, --path-like, --checksum, and --warning options. If the --id-files option is used, each ID argument
    is interpreted as the path to a file containing a list of IDs (one per line, empty lines are ignored).

    If no IDs are give, then all non-locked files with identification warnings or missing PUID will be re-identified.
    """
    if id_files:
        ids = tuple(i.strip("\n\r\t") for f in ids for i in Path(f).read_text().splitlines() if i.strip())

    if not ids:
        where: list[tuple[str, Sequence[str]]] = [("(warning is not null or puid is null) and not lock", [])]
    elif id_type in ("warnings",):
        where: list[tuple[str, Sequence[str]]] = [
            (f"{id_type} like '%\"' || ? || '\"%' and not lock", [i]) for i in ids
        ]
    elif id_type.endswith("-like"):
        id_type = id_type.removesuffix("-like")
        where: list[tuple[str, Sequence[str]]] = [(f"{id_type} like ? and not lock", [i]) for i in ids]
    else:
        where: list[tuple[str, Sequence[str]]] = [(f"{id_type} = ? and not lock", [i]) for i in ids]

    app_identify.callback(
        root,
        siegfried_path,
        siegfried_home,
        siegfried_signature,
        update_siegfried_signature,
        actions_file,
        custom_signatures_file,
        batch_size,
        update_where=where,
    )


@app.command("history", short_help="View and search events log.")
@argument("root", type=ClickPath(exists=True, file_okay=False, writable=True, resolve_path=True))
@option(
    "--from",
    "time_from",
    type=DateTime(["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f"]),
    default=None,
    help="Minimum date of events.",
)
@option(
    "--to",
    "time_to",
    type=DateTime(["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f"]),
    default=None,
    help="Maximum date of events.",
)
@option(
    "--operation",
    type=str,
    default=None,
    multiple=True,
    callback=regex_callback(r"[a-z%-]+(\.[a-z%-]+)*(:[a-z%-]+([.:][a-z%-]+)*)?", IGNORECASE),
    help="Operation and sub-operation.",
)
@option(
    "--uuid",
    type=str,
    default=None,
    multiple=True,
    callback=regex_callback(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", IGNORECASE),
    help="File UUID.",
)
@option("--reason", type=str, default=None, multiple=True, help="Event reason.")
@option(
    "--ascending/--descending",
    "ascending",
    is_flag=True,
    default=True,
    show_default=True,
    help="Sort by ascending or descending order.",
)
@pass_context
def app_history(
    ctx: Context,
    root: str,
    time_from: datetime | None,
    time_to: datetime | None,
    operation: tuple[str, ...] | None,
    uuid: tuple[str, ...] | None,
    reason: tuple[str, ...] | None,
    ascending: bool,
):
    """
    View and search events log.

    The --operation and --reason options supports LIKE syntax with the % operator.

    If multiple --uuid, --operation, or --reason options are used, the query will match any of them.

    If no query option is given, only the first 100 results will be shown.
    """
    operation = tuple(o.strip() for o in operation if o.strip(" %:.")) if operation else None
    reason = tuple(r.strip(" %") for r in reason if r.strip(" %")) if reason else None
    database_path: Path = Path(root) / "_metadata" / "files.db"

    if not database_path.is_file():
        raise FileNotFoundError(database_path)

    program_name: str = ctx.find_root().command.name
    logger_stdout: Logger = setup_logger(program_name + "_std_out", streams=[stdout])

    where: list[str] = []
    parameters: list[str | int] = []

    if time_from:
        where.append("time <= ?")
        parameters.append(time_from.isoformat())

    if time_to:
        where.append("time <= ?")
        parameters.append(time_to.isoformat())

    if uuid:
        where.append("(" + " or ".join("uuid = ?" for _ in uuid) + ")")
        parameters.extend(uuid)

    if operation:
        where.append("(" + " or ".join("operation like ?" for _ in operation) + ")")
        parameters.extend(operation)

    if reason:
        where.append("(" + " or ".join("reason like '%' || ? || '%'" for _ in reason) + ")")
        parameters.extend(reason)

    if not where:
        logger_stdout.warning(f"No selectors given. Showing {'first' if ascending else 'last'} 10000 events.")

    with FileDB(database_path) as database:
        yaml.add_representer(UUID, lambda dumper, data: dumper.represent_str(str(data)))
        yaml.add_representer(
            str,
            lambda dumper, data: (
                dumper.represent_str(str(data))
                if len(data) < 200
                else dumper.represent_scalar("tag:yaml.org,2002:str", str(data), style="|")
            ),
        )

        for event in database.history.select(
            where=" and ".join(where) or None,
            parameters=parameters or None,
            order_by=[("time", "asc" if ascending else "desc")],
            limit=None if where else 100,
        ):
            yaml.dump(event.model_dump(), stdout, yaml.Dumper, sort_keys=False)
            print()


# noinspection DuplicatedCode
@app.command("doctor", no_args_is_help=True, short_help="Inspect the database for common issues")
@argument("root", nargs=1, type=ClickPath(exists=True, file_okay=False, writable=True, resolve_path=True))
@option("--dry-run", is_flag=True, default=False, help="Show issues without fixing them.")
@pass_context
def app_doctor(ctx: Context, root: str, dry_run: bool):
    database_path: Path = Path(root) / "_metadata" / "files.db"

    if not database_path.is_file():
        raise FileNotFoundError(database_path)

    program_name: str = ctx.find_root().command.name
    logger: Logger = setup_logger(
        program_name,
        files=None if dry_run else [database_path.parent / f"{program_name}.log"],
        streams=[stdout],
    )

    with FileDB(database_path) as database:
        handle_start(ctx, database, logger)

        with ExceptionManager(BaseException) as exception:
            invalid_characters: str = '\\?%*|"<>,:;=+[]!@' + bytes(range(20)).decode("ascii") + "\x7f"
            for file in database.files.select(
                where=" or ".join("instr(relative_path, ?) != 0" for _ in invalid_characters),
                parameters=list(invalid_characters),
            ):
                file.root = Path(root)
                old_path: Path = file.relative_path
                new_path: Path = Path(
                    *[reduce(lambda acc, cur: acc.replace(cur, "_"), invalid_characters, p) for p in old_path.parts]
                )
                while file.root.joinpath(new_path).exists():
                    new_path = new_path.with_name("_" + new_path.name)
                if not dry_run:
                    file.root.joinpath(new_path).parent.mkdir(parents=True, exist_ok=True)
                    file.get_absolute_path().rename(file.root / new_path)
                    file.relative_path = new_path
                    try:
                        database.files.update(file, {"uuid": file.uuid})
                    except BaseException:
                        file.get_absolute_path().rename(file.root / old_path)
                        raise
                history: HistoryEntry = HistoryEntry.command_history(
                    ctx,
                    "file:sanitize",
                    file.uuid,
                    [str(file.relative_path), str(new_path)],
                )
                database.history.insert(history)
                history.log(INFO, logger)

        handle_end(ctx, database, exception, logger, commit=not dry_run)


@app.command("upgrade", no_args_is_help=True, short_help="Upgrade the files' database to the latest version")
@argument("root", nargs=1, type=ClickPath(exists=True, file_okay=False, writable=True, resolve_path=True))
@option(
    "--backup/--no-backup",
    is_flag=True,
    default=True,
    show_default=True,
    help="Backup the database file before upgrading.",
)
@pass_context
def app_upgrade(ctx: Context, root: str, backup: bool):
    """Upgrade the files' database to the latest version of acacore."""
    database_path: Path = Path(root) / "_metadata" / "files.db"

    if not database_path.is_file():
        raise FileNotFoundError(database_path)

    program_name: str = ctx.find_root().command.name
    logger: Logger = setup_logger(program_name, files=[database_path.parent / f"{program_name}.log"], streams=[stdout])
    is_upgraded: bool = False

    with FileDB(database_path, check_version=False) as database:
        handle_start(ctx, database, logger)

        with ExceptionManager(BaseException) as exception:
            if not is_latest(database):
                if backup:
                    backup_path = database.path.with_stem(database.path.stem + f"-{database.metadata.select().version}")
                    if backup_path.exists():
                        raise FileExistsError(f"Backup file {backup_path.name} already exists.")
                    copy2(database.path, backup_path)
                database.add_history(None, "update", [database.metadata.select().version, __acacore_version__]).log(
                    INFO, logger
                )
                database.upgrade()
                database.init()
                is_upgraded = True

        handle_end(ctx, database, exception, logger, commit=is_upgraded)


@app.group("edit", no_args_is_help=True)
def app_edit():
    """Edit a files' database."""


# noinspection DuplicatedCode
@app_edit.command("remove", no_args_is_help=True, short_help="Remove one or more files.")
@argument("root", nargs=1, type=ClickPath(exists=True, file_okay=False, writable=True, resolve_path=True))
@argument_ids(True)
@argument("reason", nargs=1, type=str, required=True)
@option("--delete", is_flag=True, default=False, help="Remove selected files from the disk.")
@pass_context
def app_edit_remove(ctx: Context, root: str, ids: tuple[str], reason: str, id_type: str, id_files: bool, delete: bool):
    """
    Remove one or more files in the files' database for the ROOT folder to EXTENSION.

    The ID arguments are interpreted as a list of UUID's by default. The behaviour can be changed with the
    --puid, --path, --path-like, --checksum, and --warning options. If the --id-files option is used, each ID argument
    is interpreted as the path to a file containing a list of IDs (one per line, empty lines are ignored).
    """
    database_path: Path = Path(root) / "_metadata" / "files.db"

    if not database_path.is_file():
        raise FileNotFoundError(database_path)

    if id_files:
        ids = tuple(i.strip("\n\r\t") for f in ids for i in Path(f).read_text().splitlines() if i.strip())

    program_name: str = ctx.find_root().command.name
    logger: Logger = setup_logger(program_name, files=[database_path.parent / f"{program_name}.log"], streams=[stdout])

    if id_type in ("warnings",):
        where: str = f"{id_type} like '%\"' || ? || '\"%'"
    elif id_type.endswith("-like"):
        id_type = id_type.removesuffix("-like")
        where: str = f"{id_type} like ?"
    else:
        where: str = f"{id_type} = ?"

    with FileDB(database_path) as database:
        handle_start(ctx, database, logger)

        with ExceptionManager(BaseException) as exception:
            for file_id in ids:
                for file in database.files.select(where=where, parameters=[file_id]):
                    history: HistoryEntry = HistoryEntry.command_history(
                        ctx,
                        "file:remove",
                        file.uuid,
                        file.model_dump(mode="json"),
                        reason,
                    )
                    database.execute(f"delete from {database.files.name} where uuid = ?", [str(file.uuid)])
                    if delete:
                        file.get_absolute_path(Path(root)).unlink(missing_ok=True)
                    database.history.insert(history)
                    history.log(INFO, logger)

        handle_end(ctx, database, exception, logger)


# noinspection DuplicatedCode
@app_edit.command("action", no_args_is_help=True, short_help="Change the action of one or more files.")
@argument("root", nargs=1, type=ClickPath(exists=True, file_okay=False, writable=True, resolve_path=True))
@argument_ids(True)
@argument(
    "action",
    metavar="ACTION",
    nargs=1,
    type=Choice(("convert", "extract", "template", "manual", "rename", "ignore", "reidentify")),
    required=True,
)
@argument("reason", nargs=1, type=str, required=True)
@option(
    "--data",
    metavar="<FIELD VALUE>",
    type=(str, str),
    multiple=True,
    help="Data to be used to replace existing action data for the specified action.",
)
@option(
    "--data-json",
    metavar="JSON",
    type=str,
    default=None,
    help="Data to be used to replace existing action data for the specified action, in JSON format.",
)
@pass_context
def app_edit_action(
    ctx: Context,
    root: str,
    ids: tuple[str],
    action: TActionType,
    reason: str,
    id_type: str,
    id_files: bool,
    data: tuple[tuple[str, str]],
    data_json: str | None,
):
    """
    Change the action of one or more files in the files' database for the ROOT folder to ACTION.

    Files are updated even if their action value is already set to ACTION.

    The ID arguments are interpreted as a list of UUID's by default. The behaviour can be changed with the
    --puid, --path, --path-like, --checksum, and --warning options. If the --id-files option is used, each ID argument
    is interpreted as the path to a file containing a list of IDs (one per line, empty lines are ignored).

    The action data for the given files is not touched unless the --data or --data-json options are used.
    The --data option takes precedence.

    \b
    Available ACTION values are:
        * convert
        * extract
        * template
        * manual
        * rename
        * ignore
        * reidentify
    """  # noqa: D301
    data_parsed: dict | list = dict(data) if data else loads(data_json) if data_json else None
    assert isinstance(data_parsed, (dict, list)), "Data is not of type dict or list"
    database_path: Path = Path(root) / "_metadata" / "files.db"

    if not database_path.is_file():
        raise FileNotFoundError(database_path)

    if id_files:
        ids = tuple(i for f in ids for i in Path(f).read_text().splitlines() if i.strip())

    program_name: str = ctx.find_root().command.name
    logger: Logger = setup_logger(program_name, files=[database_path.parent / f"{program_name}.log"], streams=[stdout])

    if id_type in ("warnings",):
        where: str = f"{id_type} like '%\"' || ? || '\"%'"
    elif id_type.endswith("-like"):
        id_type = id_type.removesuffix("-like")
        where: str = f"{id_type} like ?"
    else:
        where: str = f"{id_type} = ?"

    with FileDB(database_path) as database:
        handle_start(ctx, database, logger)

        with ExceptionManager(BaseException) as exception:
            for file_id in ids:
                for file in database.files.select(where=where, parameters=[str(file_id)]):
                    previous_action = file.action
                    file.action = action

                    if data_parsed:
                        file.action_data = file.action_data or ActionData()
                        if action == "convert":
                            file.action_data.convert = (
                                [ConvertAction.model_validate(data_parsed)]
                                if isinstance(data_parsed, dict)
                                else ActionData(convert=data_parsed).convert
                            )
                        elif action == "extract":
                            file.action_data.extract = ExtractAction.model_validate(data_parsed)
                        elif action == "template":
                            file.action_data.template = TemplateAction.model_validate(data_parsed)
                        elif action == "manual":
                            file.action_data.manual = ManualAction.model_validate(data_parsed)
                        elif action == "rename":
                            file.action_data.rename = RenameAction.model_validate(data_parsed)
                        elif action == "ignore":
                            file.action_data.ignore = IgnoreAction.model_validate(data_parsed)
                        elif action == "reidentify":
                            file.action_data.reidentify = ReIdentifyAction.model_validate(data_parsed)

                    history: HistoryEntry = HistoryEntry.command_history(
                        ctx,
                        "file:edit:action",
                        file.uuid,
                        [previous_action, action],
                        reason,
                    )
                    database.files.update(file)
                    database.history.insert(history)
                    history.log(INFO, logger)

        handle_end(ctx, database, exception, logger)


# noinspection DuplicatedCode,GrazieInspection
@app_edit.command("rename", no_args_is_help=True, short_help="Change the extension of one or more files.")
@argument("root", nargs=1, type=ClickPath(exists=True, file_okay=False, writable=True, resolve_path=True))
@argument_ids(True)
@argument(
    "extension",
    nargs=1,
    type=str,
    required=True,
    callback=regex_callback(r"^((\.[a-zA-Z0-9]+)+| +)$"),
)
@argument("reason", nargs=1, type=str, required=True)
@option("--replace", "replace_mode", flag_value="last", default=True, help="Replace the last extension.  [default]")
@option("--replace-all", "replace_mode", flag_value="all", default=True, help="Replace all extensions.")
@option("--append", "replace_mode", flag_value="append", default=True, help="Append the new extension.")
@option("--dry-run", is_flag=True, default=False, help="Show changes without committing them.")
@pass_context
def app_edit_rename(
    ctx: Context,
    root: str,
    ids: tuple[str],
    extension: str,
    reason: str,
    replace_mode: str,
    id_type: str,
    id_files: bool,
    dry_run: bool,
):
    r"""
    Change the extension of one or more files in the files' database for the ROOT folder to EXTENSION.

    The ID arguments are interpreted as a list of UUID's by default. The behaviour can be changed with the
    --puid, --path, --path-like, --checksum, and --warning options. If the --id-files option is used, each ID argument
    is interpreted as the path to a file containing a list of IDs (one per line, empty lines are ignored).

    To see the changes without committing them, use the --dry-run option.

    The --replace and --replace-all options will only replace valid suffixes (i.e., matching the expression
    \.[a-zA-Z0-9]+).

    The --append option will not add the new extension if it is already present.
    """
    extension = extension.strip()
    database_path: Path = Path(root) / "_metadata" / "files.db"

    if not database_path.is_file():
        raise FileNotFoundError(database_path)

    if id_files:
        ids = tuple(i for f in ids for i in Path(f).read_text().splitlines() if i.strip())

    program_name: str = ctx.find_root().command.name
    logger: Logger = setup_logger(
        program_name,
        files=None if dry_run else [database_path.parent / f"{program_name}.log"],
        streams=[stdout],
    )

    if id_type in ("warnings",):
        where: str = f"{id_type} like '%\"' || ? || '\"%'"
    elif id_type.endswith("-like"):
        id_type = id_type.removesuffix("-like")
        where: str = f"{id_type} like ?"
    else:
        where: str = f"{id_type} = ?"

    with FileDB(database_path) as database:
        handle_start(ctx, database, logger)

        with ExceptionManager(BaseException) as exception:
            for file_id in ids:
                for file in database.files.select(where=where, parameters=[str(file_id)]):
                    history: HistoryEntry = HistoryEntry.command_history(ctx, "file:edit:rename", file.uuid, [], reason)
                    old_name: str = file.relative_path.name
                    new_name: str = old_name

                    if replace_mode == "last" and not match(r"^\.[a-zA-Z0-9]+$", file.relative_path.suffix):
                        new_name = file.relative_path.name + extension
                    elif replace_mode == "last":
                        new_name = file.relative_path.with_suffix(extension).name
                    elif replace_mode == "append" and old_name.lower().endswith(extension.lower()):
                        continue
                    elif replace_mode == "append":
                        new_name = file.relative_path.name + extension
                    elif replace_mode == "all":
                        suffixes: str = ""
                        for suffix in file.relative_path.suffixes[::-1]:
                            if match(r"^\.[a-zA-Z0-9]+$", suffix):
                                suffixes = suffix + suffixes
                            else:
                                break
                        new_name = file.relative_path.name.removesuffix(suffixes) + extension

                    if new_name.lower() == old_name.lower():
                        continue

                    history.data = [str(old_name), str(new_name)]

                    if dry_run:
                        history.log(INFO, logger)
                        continue

                    file.root = Path(root)
                    file.get_absolute_path().rename(file.get_absolute_path().with_name(new_name))
                    file.relative_path = file.relative_path.with_name(new_name)

                    try:
                        database.files.update(file, {"relative_path": file.relative_path.with_name(old_name)})
                    except SQLiteError:
                        file.get_absolute_path().rename(file.get_absolute_path().with_name(old_name))
                        file.relative_path = file.relative_path.with_name(old_name)

                    history.log(INFO, logger)
                    database.history.insert(history)

        handle_end(ctx, database, exception, logger)


@app_edit.command("rollback", no_args_is_help=True, short_help="Roll back edits.")
@argument("root", nargs=1, type=ClickPath(exists=True, file_okay=False, writable=True, resolve_path=True))
@argument(
    "time_from",
    metavar="FROM",
    nargs=1,
    type=DateTime(["%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f"]),
    required=True,
)
@argument(
    "time_to",
    metavar="TO",
    nargs=1,
    type=DateTime(["%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f"]),
    required=True,
)
@argument("reason", nargs=1, type=str, required=True)
@pass_context
def app_edit_rollback(ctx: Context, root: str, time_from: datetime, time_to: datetime, reason: str):
    """
    Roll back edits between two times.

    FROM and TO timestamps must be in the format '%Y-%m-%dT%H:%M:%S' or '%Y-%m-%dT%H:%M:%S.%f'.
    """
    database_path: Path = Path(root) / "_metadata" / "files.db"

    if not database_path.is_file():
        raise FileNotFoundError(database_path)

    program_name: str = ctx.find_root().command.name
    logger: Logger = setup_logger(program_name, files=[database_path.parent / f"{program_name}.log"], streams=[stdout])

    with FileDB(database_path) as database:
        handle_start(ctx, database, logger)

        with ExceptionManager(BaseException) as exception:
            where: str = "uuid is not null and time >= ?"
            parameters: list[str] = [time_from.isoformat()]

            if time_to:
                where += " and time <= ?"
                parameters.append(time_to.isoformat())

            for event in database.history.select(where=where, parameters=parameters, order_by=[("time", "desc")]):
                command: HistoryEntry = HistoryEntry.command_history(ctx, "file", reason=reason)
                event_command, _, event_operation = event.operation.partition(":")
                file: File | None = None

                if event_command == f"{program_name}.{app_edit.name}.{app_edit_action.name}":
                    file = database.files.select(where="uuid = ?", parameters=[str(event.uuid)]).fetchone()
                    if file:
                        old_action, _ = event.data
                        database.files.update({"action": old_action}, {"uuid": file.uuid})
                elif event_command == f"{program_name}.{app_edit.name}.{app_edit_remove.name}":
                    file = File.model_validate(event.data)
                    database.files.insert(file)
                elif event_command == f"{program_name}.{app_edit.name}.{app_edit_rename.name}":
                    file = database.files.select(where="uuid = ?", parameters=[str(event.uuid)]).fetchone()
                    if file:
                        old_name, new_name = event.data
                        file.root = Path(root)
                        file.get_absolute_path().rename(file.get_absolute_path().with_name(old_name))
                        file.relative_path = file.relative_path.with_name(old_name)
                        try:
                            database.files.update({"relative_path": file.relative_path}, {"uuid": file.uuid})
                        except DatabaseError:
                            file.get_absolute_path().rename(file.get_absolute_path().with_name(new_name))
                            file.relative_path = file.relative_path.with_name(new_name)

                if file:
                    command.uuid = file.uuid
                    command.data = [event.uuid, event.time, event.operation]
                    database.history.insert(command)

                command.log(INFO, logger)

        handle_end(ctx, database, exception, logger)
