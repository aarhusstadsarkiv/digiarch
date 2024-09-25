from itertools import islice
from logging import INFO
from os import environ
from pathlib import Path
from re import compile as re_compile
from re import error as re_error
from re import IGNORECASE
from traceback import format_tb
from typing import Generator
from typing import get_args as get_type_args
from typing import Pattern
from typing import Sequence
from uuid import UUID
from uuid import uuid4

from acacore.database import FileDB
from acacore.exceptions.files import IdentificationError
from acacore.models.file import File
from acacore.models.history import HistoryEntry
from acacore.models.reference_files import Action
from acacore.models.reference_files import ActionData
from acacore.models.reference_files import CustomSignature
from acacore.models.reference_files import ManualAction
from acacore.models.reference_files import RenameAction
from acacore.siegfried import Siegfried
from acacore.siegfried.siegfried import SiegfriedFile
from acacore.siegfried.siegfried import TSignaturesProvider
from acacore.utils.click import check_database_version
from acacore.utils.click import end_program
from acacore.utils.click import start_program
from acacore.utils.functions import find_files
from acacore.utils.helpers import ExceptionManager
from click import Argument
from click import BadParameter
from click import Choice
from click import command
from click import Context
from click import IntRange
from click import option
from click import pass_context
from click import Path as ClickPath
from PIL import UnidentifiedImageError

from digiarch.__version__ import __version__
from digiarch.commands.edit.common import argument_query
from digiarch.commands.edit.common import query_to_where
from digiarch.commands.edit.common import TQuery
from digiarch.common import argument_root
from digiarch.common import ctx_params
from digiarch.common import fetch_actions
from digiarch.common import fetch_custom_signatures


def handle_rename_action(file: File, action: RenameAction) -> tuple[Path, Path] | tuple[None, None]:
    old_name: str = file.name
    new_name: str = old_name

    if action.on_extension_mismatch and "extension mismatch" not in (file.warning or []):
        return None, None

    if not action.append:
        new_name = old_name.removesuffix(file.suffixes)

    new_name += "." + action.extension.removeprefix(".")

    old_path: Path = file.get_absolute_path()
    new_path: Path = old_path.with_name(new_name)
    if old_path == new_path:
        return None, None
    old_path.rename(new_path)
    return old_path, new_path


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
    parent: UUID | None = None,
    processed: bool = False,
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
        UnidentifiedImageError,
        allow=[OSError, IOError],
    ) as identify_error:
        file: File = File.from_file(
            path,
            root,
            siegfried_result or siegfried,
            actions,
            custom_signatures,
            uuid=uuid,
            processed=processed,
        )

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
                "error",
                file.uuid,
                repr(identify_error.exception),
                "".join(format_tb(identify_error.traceback)) if identify_error.traceback else None,
            ),
        )

    if file.action_data and file.action_data.rename:
        old_path, new_path = handle_rename_action(file, file.action_data.rename)
        if new_path:
            file, file_history = identify_file(
                ctx,
                root,
                new_path,
                database,
                siegfried,
                None,
                actions,
                custom_signatures,
                update=update,
            )
            if not file:
                return None, []
            file_history.insert(
                0,
                HistoryEntry.command_history(
                    ctx,
                    "rename",
                    file.uuid,
                    [str(old_path.relative_to(root)), str(new_path.relative_to(root))],
                ),
            )
            return file, file_history

    if parent:
        file.parent = parent

    file.processed = False

    if update:
        database.files.update(file, {"uuid": file.uuid})
    else:
        database.files.insert(file, exist_ok=True)

    return file, file_history


@command("identify", no_args_is_help=True, short_help="Identify files.")
@argument_root(False)
@option(
    "--siegfried-path",
    type=ClickPath(exists=True, dir_okay=False, resolve_path=True),
    envvar="SIEGFRIED_PATH",
    default=Path(environ.get("GOPATH", "go"), "bin", "sf"),
    required=True,
    show_envvar=True,
    callback=lambda _ctx, _param, value: Path(value),
    help="The path to the Siegfried executable.",
)
@option(
    "--siegfried-home",
    type=ClickPath(exists=True, file_okay=False, resolve_path=True),
    envvar="SIEGFRIED_HOME",
    required=True,
    show_envvar=True,
    callback=lambda _ctx, _param, value: Path(value),
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
    "--actions",
    "actions_file",
    type=ClickPath(exists=True, dir_okay=False, file_okay=True, resolve_path=True),
    envvar="DIGIARCH_ACTIONS",
    show_envvar=True,
    default=None,
    callback=lambda _ctx, _param, value: Path(value) if value else None,
    help="Path to a YAML file containing file format actions.",
)
@option(
    "--custom-signatures",
    "custom_signatures_file",
    type=ClickPath(exists=True, dir_okay=False, file_okay=True, resolve_path=True),
    envvar="DIGIARCH_CUSTOM_SIGNATURES",
    show_envvar=True,
    default=None,
    callback=lambda _ctx, _param, value: Path(value) if value else None,
    help="Path to a YAML file containing custom signature specifications.",
)
@option(
    "--exclude",
    type=str,
    multiple=True,
    help="Glob pattern for file and folder names to exclude.  [multiple]",
)
@option("--batch-size", type=IntRange(1), default=100)
@pass_context
def command_identify(
    ctx: Context,
    root: Path,
    siegfried_path: Path,
    siegfried_home: Path,
    siegfried_signature: TSignaturesProvider,
    actions_file: Path | None,
    custom_signatures_file: Path | None,
    exclude: tuple[str, ...],
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
    try:
        exclude_patterns: list[Pattern[str]] = [
            re_compile(ex.strip().replace(".", "\\.").replace("*", ".*"), IGNORECASE) for ex in exclude if ex.strip()
        ]
    except re_error as err:
        raise BadParameter(f"{err.args[0] if err.args else err.msg} {err.pattern}", ctx, ctx_params(ctx)["exclude"])

    # noinspection DuplicatedCode
    check_database_version(ctx, ctx_params(ctx)["root"], (db_path := root / "_metadata" / "files.db"))

    siegfried = Siegfried(
        siegfried_path or Path(environ["GOPATH"], "bin", "sf"),
        f"{siegfried_signature}.sig",
        siegfried_home,
    )

    try:
        siegfried.run("-version", "-sig", siegfried.signature)
    except IdentificationError as err:
        print(err)
        raise BadParameter("Invalid binary or signature file.", ctx, ctx_params(ctx)["siegfried_path"])

    actions = fetch_actions(ctx, "actions_file", actions_file)
    custom_signatures = fetch_custom_signatures(ctx, "custom_signatures_file", custom_signatures_file)

    db_path.parent.mkdir(parents=True, exist_ok=True)

    with FileDB(db_path) as database:
        log_file, log_stdout, _ = start_program(ctx, database, __version__, None, True, True, False)
        database.init()

        with ExceptionManager(BaseException) as exception:
            files: Generator[Path, None, None]

            if update_where:
                files = (
                    f.get_absolute_path(root)
                    for w, p in update_where
                    for f in database.files.select(where=w, parameters=p)
                )
            else:
                files = find_files(root, exclude=[database.path.parent])

            while batch := list(islice(files, batch_size)):
                if exclude_patterns and not update_where:
                    batch = [
                        p
                        for p in batch
                        if not any(
                            any(exp.match(part) for exp in exclude_patterns) for part in p.relative_to(root).parts
                        )
                    ]
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
                            "update" if update_where else "new",
                            file.uuid,
                        ).log(
                            INFO,
                            log_stdout,
                            puid=file.puid,
                            action=file.action,
                            path=file.relative_path,
                        )

                    for event in file_history:
                        event.log(INFO, log_stdout)
                        database.history.insert(event)

        end_program(ctx, database, exception, False, log_file, log_stdout)


@command("reidentify", no_args_is_help=True, short_help="Reidentify files.")
@argument_root(True)
@argument_query(False, "uuid", ["uuid", "checksum", "puid", "relative_path", "action", "warning", "processed", "lock"])
@pass_context
def command_reidentify(
    _ctx: Context,
    root: str | Path,
    siegfried_path: Path,
    siegfried_home: Path,
    siegfried_signature: TSignaturesProvider,
    actions_file: Path | None,
    custom_signatures_file: Path | None,
    batch_size: int,
    query: TQuery,
):
    """
    Re-indentify specific files in the ROOT folder.

    Each file is re-identified with Siegfried and an action is assigned to it.
    Files that need re-identification with custom signatures, renaming, or ignoring are processed accordingly.

    For details on the QUERY argument, see the edit command.

    If there is no query, then all non-locked files with identification warnings or no PUID will be re-identified.
    """
    if query:
        where, params = query_to_where(query)
    else:
        where, params = "(warning is not null or puid is null) and not lock", []

    command_identify.callback(
        root,
        siegfried_path,
        siegfried_home,
        siegfried_signature,
        actions_file,
        custom_signatures_file,
        (),
        batch_size,
        update_where=[(where, params)],
    )


command_reidentify.params = [p for p in command_identify.params if p.name != "root"] + command_reidentify.params
command_reidentify.params = [
    *(p for p in command_reidentify.params if isinstance(p, Argument)),
    *(p for p in command_identify.params if p.name not in [p_.name for p_ in command_reidentify.params]),
    *(p for p in command_reidentify.params if not isinstance(p, Argument) and p.name != "exclude"),
]
