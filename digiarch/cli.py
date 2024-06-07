from datetime import datetime
from json import loads
from logging import Logger
from os import environ
from pathlib import Path
from re import match
from sqlite3 import Error as SQLiteError
from sys import stdout
from traceback import format_tb
from typing import Optional
from typing import Union

import yaml
from acacore.__version__ import __version__ as __acacore_version__
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
from acacore.models.reference_files import ReplaceAction
from acacore.models.reference_files import TActionType
from acacore.reference_files import get_actions
from acacore.reference_files import get_custom_signatures
from acacore.siegfried import Siegfried
from acacore.siegfried.siegfried import TSignature
from acacore.utils.functions import find_files
from acacore.utils.helpers import ExceptionManager
from acacore.utils.log import setup_logger
from click import argument
from click import Choice
from click import Context
from click import DateTime
from click import group
from click import option
from click import pass_context
from click import Path as ClickPath
from click import version_option
from PIL import Image
from PIL import UnidentifiedImageError
from PIL.Image import DecompressionBombError
from pydantic import TypeAdapter

from .__version__ import __version__
from .database import FileDB

Image.MAX_IMAGE_PIXELS = int(50e3**2)


def handle_rename(file: File, action: RenameAction) -> Union[tuple[Path, Path], tuple[None, None]]:
    old_path: Path = file.get_absolute_path()

    if action.on_extension_mismatch and (not file.warning or "extension mismatch" not in file.warning):
        return None, None

    new_suffixes: list[str] = [action.extension] if not action.append else [*old_path.suffixes, action.extension]
    new_path: Path = old_path.with_suffix("".join(new_suffixes))
    if old_path == new_path:
        return None, None
    old_path.rename(new_path)
    return old_path, new_path


def handle_start(ctx: Context, database: FileDB, *loggers: Logger):
    program_start: HistoryEntry = HistoryEntry.command_history(
        ctx,
        "start",
        data=ctx.params | {"version": __version__, "acacore": __acacore_version__},
    )

    database.history.insert(program_start)

    for logger in loggers:
        logger.info(program_start.operation)


def handle_end(ctx: Context, database: FileDB, exception: ExceptionManager, *loggers: Logger, commit: bool = True):
    program_end: HistoryEntry = HistoryEntry.command_history(
        ctx,
        "end",
        data=repr(exception.exception) if exception.exception else None,
        reason="".join(format_tb(exception.traceback)) if exception.traceback else None,
    )

    for logger in loggers:
        if exception.exception:
            logger.error(f"{program_end.operation} {exception.exception!r}")
        else:
            logger.info(program_end.operation)

    if database.is_open:
        database.history.insert(program_end)
        if commit:
            database.commit()


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
    type=Choice(("pronom", "loc", "tika", "freedesktop", "pronom-tika-loc", "deluxe", "archivematica")),
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
    default=None,
    help="Path to a YAML file containing file format actions.",
)
@option(
    "--custom-signatures",
    "custom_signatures_file",
    type=ClickPath(exists=True, dir_okay=False, file_okay=True, resolve_path=True),
    default=None,
    help="Path to a YAML file containing custom signature specifications.",
)
@pass_context
def app_identify(
    ctx: Context,
    root: Union[str, Path],
    siegfried_path: Optional[str],
    siegfried_home: Optional[str],
    siegfried_signature: TSignature,
    update_siegfried_signature: bool,
    actions_file: Optional[str],
    custom_signatures_file: Optional[str],
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
            for path in find_files(root, exclude=[database_path.parent]):
                if database.file_exists(path, root):
                    continue

                file_history: list[HistoryEntry] = []

                with ExceptionManager(
                    Exception,
                    UnidentifiedImageError,
                    DecompressionBombError,
                    allow=[OSError, IOError],
                ) as identify_error:
                    file = File.from_file(path, root, siegfried, actions, custom_signatures)

                if identify_error.exception:
                    file = File.from_file(path, root, siegfried)
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
                        file = File.from_file(new_path, root, siegfried, actions, custom_signatures)
                        file_history.append(
                            HistoryEntry.command_history(
                                ctx,
                                "file:action:rename",
                                file.uuid,
                                [old_path.relative_to(root), new_path.relative_to(root)],
                            ),
                        )

                database.files.insert(file, exist_ok=True)

                logger_stdout.info(
                    f"{HistoryEntry.command_history(ctx, ':file:new').operation} "
                    f"{file.relative_path} {file.puid} {file.action}",
                )

                for entry in file_history:
                    logger.info(f"{entry.operation} {entry.uuid}")
                    database.history.insert(entry)

        handle_end(ctx, database, exception, logger)


@app.group("edit", no_args_is_help=True)
def app_edit():
    """Edit a files' database."""


# noinspection DuplicatedCode
@app_edit.command("remove", no_args_is_help=True, short_help="Remove one or more files.")
@argument("root", nargs=1, type=ClickPath(exists=True, file_okay=False, writable=True, resolve_path=True))
@argument(
    "ids",
    metavar="ID...",
    nargs=-1,
    type=str,
    required=True,
    callback=lambda _c, _p, v: tuple(sorted(set(v), key=v.index)),
)
@argument("reason", nargs=1, type=str, required=True)
@option("--uuid", "id_type", flag_value="uuid", default=True, help="Use UUID's as identifiers.  [default]")
@option("--puid", "id_type", flag_value="puid", help="Use PUID's as identifiers.")
@option("--path", "id_type", flag_value="relative_path", help="Use relative paths as identifiers.")
@option(
    "--path-like",
    "id_type",
    flag_value="relative_path-like",
    help="Use relative paths as identifiers, match with LIKE.",
)
@option("--checksum", "id_type", flag_value="checksum", help="Use checksums as identifiers.")
@option("--warning", "id_type", flag_value="warnings", help="Use warnings as identifiers.")
@option("--id-files", is_flag=True, default=False, help="Interpret IDs as files from which to read the IDs.")
@pass_context
def app_edit_remove(ctx: Context, root: str, ids: tuple[str], reason: str, id_type: str, id_files: bool):
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
                history: HistoryEntry = HistoryEntry.command_history(ctx, "file:remove")

                file: Optional[File] = None
                for file in database.files.select(where=where, parameters=[file_id]):
                    history.uuid = file.uuid
                    history.data = file.model_dump(mode="json")
                    history.reason = reason
                    database.execute(f"delete from {database.files.name} where uuid = ?", [str(file.uuid)])
                    database.history.insert(history)
                    logger.info(f"{history.operation} {file.uuid} {file.relative_path} {history.reason}")

                if file is None:
                    logger.error(f"{history.operation} {id_type} {file_id} not found")

        handle_end(ctx, database, exception, logger)


# noinspection DuplicatedCode
@app_edit.command("action", no_args_is_help=True, short_help="Change the action of one or more files.")
@argument("root", nargs=1, type=ClickPath(exists=True, file_okay=False, writable=True, resolve_path=True))
@argument(
    "ids",
    metavar="ID...",
    nargs=-1,
    type=str,
    required=True,
    callback=lambda _c, _p, v: tuple(sorted(set(v), key=v.index)),
)
@argument(
    "action",
    metavar="ACTION",
    nargs=1,
    type=Choice(("convert", "extract", "replace", "manual", "rename", "ignore", "reidentify")),
    required=True,
)
@argument("reason", nargs=1, type=str, required=True)
@option("--uuid", "id_type", flag_value="uuid", default=True, help="Use UUID's as identifiers.  [default]")
@option("--puid", "id_type", flag_value="puid", help="Use PUID's as identifiers.")
@option("--path", "id_type", flag_value="relative_path", help="Use relative paths as identifiers.")
@option(
    "--path-like",
    "id_type",
    flag_value="relative_path-like",
    help="Use relative paths as identifiers, match with LIKE.",
)
@option("--checksum", "id_type", flag_value="checksum", help="Use checksums as identifiers.")
@option("--warning", "id_type", flag_value="warnings", help="Use warnings as identifiers.")
@option("--id-files", is_flag=True, default=False, help="Interpret IDs as files from which to read the IDs.")
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
    data_json: Optional[str],
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
        * replace
        * manual
        * rename
        * ignore
        * reidentify
    """  # noqa: D301
    data_parsed: Optional[Union[dict, list]] = dict(data) if data else loads(data_json) if data_json else None
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
                history: HistoryEntry = HistoryEntry.command_history(ctx, "file:edit:action")
                file: Optional[File] = None

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
                        elif action == "replace":
                            file.action_data.replace = ReplaceAction.model_validate(data_parsed)
                        elif action == "manual":
                            file.action_data.manual = ManualAction.model_validate(data_parsed)
                        elif action == "rename":
                            file.action_data.rename = RenameAction.model_validate(data_parsed)
                        elif action == "ignore":
                            file.action_data.ignore = IgnoreAction.model_validate(data_parsed)
                        elif action == "reidentify":
                            file.action_data.reidentify = ReIdentifyAction.model_validate(data_parsed)

                    history.uuid = file.uuid
                    history.data = [previous_action, action]
                    history.reason = reason
                    database.files.update(file)
                    logger.info(f"{history.operation} {file.uuid} {file.relative_path} {history.data} {history.reason}")
                    database.history.insert(history)

                if file is None:
                    logger.error(f"{history.operation} {id_type} {file_id} not found")

        handle_end(ctx, database, exception, logger)


# noinspection DuplicatedCode
@app_edit.command("rename", no_args_is_help=True, short_help="Change the extension of one or more files.")
@argument("root", nargs=1, type=ClickPath(exists=True, file_okay=False, writable=True, resolve_path=True))
@argument(
    "ids",
    metavar="ID...",
    nargs=-1,
    type=str,
    required=True,
    callback=lambda _c, _p, v: tuple(sorted(set(v), key=v.index)),
)
@argument("extension", nargs=1, type=str, required=True)
@argument("reason", nargs=1, type=str, required=True)
@option("--uuid", "id_type", flag_value="uuid", default=True, help="Use UUID's as identifiers.  [default]")
@option("--puid", "id_type", flag_value="puid", help="Use PUID's as identifiers.")
@option("--path", "id_type", flag_value="relative_path", help="Use relative paths as identifiers.")
@option(
    "--path-like",
    "id_type",
    flag_value="relative_path-like",
    help="Use relative paths as identifiers, match with LIKE.",
)
@option("--checksum", "id_type", flag_value="checksum", help="Use checksums as identifiers.")
@option("--warning", "id_type", flag_value="warnings", help="Use warnings as identifiers.")
@option("--id-files", is_flag=True, default=False, help="Interpret IDs as files from which to read the IDs.")
@pass_context
def app_edit_rename(
    ctx: Context,
    root: str,
    ids: tuple[str],
    extension: str,
    reason: str,
    id_type: str,
    id_files: bool,
):
    """
    Change the extension of one or more files in the files' database for the ROOT folder to EXTENSION.

    The ID arguments are interpreted as a list of UUID's by default. The behaviour can be changed with the
    --puid, --path, --path-like, --checksum, and --warning options. If the --id-files option is used, each ID argument
    is interpreted as the path to a file containing a list of IDs (one per line, empty lines are ignored).

    \b
    The EXTENSION argument supports formatting using f-string syntax:
        * suffix - the last suffix of the file, including leading period (file.ext1.ext2 -> .ext2)
        * prefixes - all the suffixes of the file except for the last one, including leading periods (file.ext1.ext2 -> .ext1)
        * suffixes - all the suffixes of the file, including leading periods (file.ext1.ext2 -> .ext1.ext2)
    """  # noqa: D301
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
                history: HistoryEntry = HistoryEntry.command_history(ctx, "file:edit:rename")
                file: Optional[File] = None

                for file in database.files.select(where=where, parameters=[str(file_id)]):
                    old_ext: str = "".join(file.relative_path.suffixes)
                    new_ext: str = extension.format(
                        suffix=file.relative_path.suffix,
                        prefixes="".join(file.relative_path.suffixes[:-1]),
                        suffixes="".join(file.relative_path.suffixes),
                    ).strip()

                    if new_ext and not match(r'(\.[^/<>:"\\|?*\x7F\x00-\x20]+)+', new_ext):
                        raise ValueError(f"Invalid suffix {new_ext!r}")

                    if new_ext.lower() == old_ext.lower() or new_ext.lower() == old_ext.lower() + old_ext.lower():
                        continue

                    old_name: str = file.relative_path.name
                    new_name: str = file.relative_path.name.removesuffix(old_ext) + new_ext

                    file.root = Path(root)
                    file.get_absolute_path().rename(file.get_absolute_path().with_name(new_name))
                    file.relative_path = file.relative_path.with_name(new_name)

                    history.uuid = file.uuid
                    history.data = [str(old_name), str(new_name)]
                    history.reason = reason

                    try:
                        database.files.update(file, {"relative_path": file.relative_path.with_name(old_name)})
                    except SQLiteError:
                        file.get_absolute_path().rename(file.get_absolute_path().with_name(old_name))
                        file.relative_path = file.relative_path.with_name(old_name)

                    logger.info(f"{history.operation} {file.uuid} {file.relative_path} {history.data} {history.reason}")
                    database.history.insert(history)

                if file is None:
                    logger.error(f"{history.operation} {id_type} {file_id} not found")

        handle_end(ctx, database, exception, logger)


@app_edit.command("rollback", no_args_is_help=True, short_help="Rollback edit.")
@argument("root", nargs=1, type=ClickPath(exists=True, file_okay=False, writable=True, resolve_path=True))
@argument(
    "timestamp",
    nargs=1,
    type=DateTime(
        [
            "%Y-%m-%d",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S.%f",
            "%Y-%m-%d %H:%M:%S.%s",
        ]
    ),
    required=True,
)
@argument(
    "max_time",
    nargs=1,
    type=DateTime(
        [
            "%Y-%m-%d",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S.%f",
            "%Y-%m-%d %H:%M:%S.%s",
        ]
    ),
    required=True,
)
@argument("reason", nargs=1, type=str, required=True)
@pass_context
def app_edit_rollback(ctx: Context, root: str, timestamp: datetime, max_time: datetime, reason: str):
    database_path: Path = Path(root) / "_metadata" / "files.db"

    if not database_path.is_file():
        raise FileNotFoundError(database_path)

    program_name: str = ctx.find_root().command.name
    logger: Logger = setup_logger(program_name, files=[database_path.parent / f"{program_name}.log"], streams=[stdout])

    with FileDB(database_path) as database:
        handle_start(ctx, database, logger)

        with ExceptionManager(BaseException) as exception:
            command: HistoryEntry = HistoryEntry.command_history(ctx, "file", reason=reason)
            where: str = "uuid is not null and time >= ?"
            parameters: list[str] = [timestamp.isoformat()]

            if max_time:
                where += " and time <= ?"
                parameters.append(max_time.isoformat())

            for event in database.history.select(where=where, parameters=parameters, order_by=[("time", "desc")]):
                event_command, _, event_operation = event.operation.partition(":")
                file: Optional[File] = None

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
                        database.files.update({"relative_path": file.relative_path}, {"uuid": file.uuid})

                if file:
                    command.uuid = file.uuid
                    command.data = [event.uuid, event.time, event.operation]
                    database.history.insert(command)

                logger.info(
                    f"{command.operation}{'' if file else ':error'} {event.uuid} {event.time} {event.operation}"
                )

        handle_end(ctx, database, exception, logger)
