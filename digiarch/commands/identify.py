from itertools import islice
from logging import ERROR
from logging import INFO
from logging import Logger
from pathlib import Path
from traceback import format_tb
from typing import get_args as get_type_args
from uuid import UUID

from acacore.database import FilesDB
from acacore.exceptions.files import IdentificationError
from acacore.models.event import Event
from acacore.models.file import OriginalFile
from acacore.models.reference_files import Action
from acacore.models.reference_files import ActionData
from acacore.models.reference_files import CustomSignature
from acacore.models.reference_files import ManualAction
from acacore.siegfried import Siegfried
from acacore.siegfried.siegfried import SiegfriedFile
from acacore.siegfried.siegfried import TSignaturesProvider
from acacore.utils.click import ctx_params
from acacore.utils.click import end_program
from acacore.utils.click import start_program
from acacore.utils.functions import find_files
from acacore.utils.helpers import ExceptionManager
from click import BadParameter
from click import Choice
from click import Context
from click import group
from click import IntRange
from click import option
from click import pass_context
from click import Path as ClickPath
from PIL import UnidentifiedImageError

from digiarch.__version__ import __version__
from digiarch.common import AVID
from digiarch.common import fetch_actions
from digiarch.common import fetch_custom_signatures
from digiarch.common import open_database
from digiarch.common import option_avid
from digiarch.common import option_dry_run


def identify_requirements(
    ctx: Context,
    siegfried_path: str | None,
    siegfried_signature: str,
    siegfried_home: str | None,
    actions_file: str | None,
    custom_signatures_file: str | None,
) -> tuple[Siegfried, dict[str, Action], list[CustomSignature]]:
    siegfried = Siegfried(
        siegfried_path or "sf",
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

    return siegfried, actions, custom_signatures


def identify_original_file(
    ctx: Context,
    avid: AVID,
    db: FilesDB,
    siegfried_file: SiegfriedFile,
    actions: dict[str, Action],
    custom_signatures: list[CustomSignature],
    dry_run: bool,
    update: bool,
    parent: UUID | None,
    *loggers: Logger,
):
    errors: list[Event] = []

    with ExceptionManager(Exception, UnidentifiedImageError, allow=[OSError, IOError]) as error:
        file = OriginalFile.from_file(
            siegfried_file.filename,
            avid.path,
            siegfried_file,
            actions,
            custom_signatures,
            parent=parent,
        )

    if error.exception:
        file = OriginalFile.from_file(
            siegfried_file.filename,
            avid.path,
            siegfried_file,
            parent=parent,
        )
        file.action = "manual"
        file.action_data = ActionData(manual=ManualAction(reason=repr(error.exception), process=""))
        errors.append(
            Event.from_command(
                ctx,
                "error",
                (file.uuid, "original"),
                repr(error.exception),
                "".join(format_tb(error.traceback)).strip() or None,
            )
        )

    existing_file: OriginalFile | None = db.original_files[file]

    if existing_file and update:
        file.uuid = existing_file.uuid
        file.original_path = existing_file.original_path
        file.parent = file.parent or existing_file.parent
        file.processed = (
            False
            if file.action != existing_file.action or file.action_data != existing_file.action_data
            else existing_file.processed
        )
        file.lock = existing_file.lock

    if not dry_run and (update or not existing_file):
        db.original_files.insert(file, on_exists="replace" if update else "error")
        db.log.insert(*errors)

    if update or not existing_file:
        Event.from_command(ctx, "file", (file.uuid, "original")).log(
            INFO,
            *loggers,
            puid=str(file.puid).ljust(10),
            action=str(file.action).ljust(7),
            path=file.relative_path.relative_to(avid.dirs.original_documents.name),
        )

        for error in errors:
            error.log(ERROR, show_args=["uuid", "data"])


def identify_original_files(
    ctx: Context,
    avid: AVID,
    db: FilesDB,
    siegfried: Siegfried,
    paths: list[Path],
    actions: dict[str, Action],
    custom_signatures: list[CustomSignature],
    dry_run: bool,
    update: bool,
    parent: UUID | None,
    *loggers: Logger,
):
    if not paths:
        return
    for sf_file in siegfried.identify(*paths).files:
        identify_original_file(
            ctx,
            avid,
            db,
            sf_file,
            actions,
            custom_signatures,
            dry_run,
            update,
            parent,
            *loggers,
        )


@group("identify", no_args_is_help=True, short_help="Identify files.")
def grp_identify():
    pass


@grp_identify.command("original")
@option(
    "--siegfried-path",
    type=ClickPath(exists=True, dir_okay=False, resolve_path=True),
    envvar="SIEGFRIED_PATH",
    default=None,
    required=False,
    show_envvar=True,
    help="The path to the Siegfried executable.",
)
@option(
    "--siegfried-home",
    type=ClickPath(exists=True, file_okay=False, resolve_path=True),
    envvar="SIEGFRIED_HOME",
    required=True,
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
@option("--exclude", type=str, multiple=True, help="File and folder names to exclude.  [multiple]")
@option("--batch-size", type=IntRange(1), default=100, show_default=True, help="Amount of files to identofy at a time.")
@option_avid()
@option_dry_run()
@pass_context
def cmd_identify_original(
    ctx: Context,
    avid: AVID,
    siegfried_path: str | None,
    siegfried_signature: str,
    siegfried_home: str | None,
    actions_file: str | None,
    custom_signatures_file: str | None,
    exclude: tuple[str, ...],
    batch_size: int | None,
    dry_run: bool,
):
    siegfried, actions, custom_signatures = identify_requirements(
        ctx,
        siegfried_path,
        siegfried_signature,
        siegfried_home,
        actions_file,
        custom_signatures_file,
    )

    with open_database(ctx, avid) as db:
        log_file, log_stdout, _ = start_program(ctx, db, __version__, None, not dry_run, True, dry_run)

        with ExceptionManager(BaseException) as exception:
            files = find_files(avid.dirs.original_documents)
            while batch := list(islice(files, batch_size)):
                if exclude:
                    batch = [f for f in batch if not any(p in exclude for p in f.parts)]

                identify_original_files(
                    ctx,
                    avid,
                    db,
                    siegfried,
                    batch,
                    actions,
                    custom_signatures,
                    dry_run,
                    False,
                    None,
                    log_stdout,
                )

        end_program(ctx, db, exception, dry_run, log_file, log_stdout)
