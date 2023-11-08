from logging import Logger
from os import environ
from pathlib import Path
from sys import stdout
from traceback import format_tb
from typing import Optional
from typing import Union

from acacore.models.file import File
from acacore.models.history import HistoryEntry
from acacore.models.reference_files import RenameAction
from acacore.reference_files import get_actions
from acacore.reference_files import get_custom_signatures
from acacore.siegfried import Siegfried
from acacore.siegfried.siegfried import TSignature
from acacore.utils.functions import find_files
from acacore.utils.helpers import ExceptionManager
from acacore.utils.log import setup_logger
from click import Choice
from click import Context
from click import Path as ClickPath
from click import argument
from click import group
from click import option
from click import pass_context
from click import version_option

from .database import FileDB


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


@group("digiarch")
@version_option("")
def app():
    pass


@app.command("process")
@argument(
    "root",
    type=ClickPath(exists=True, file_okay=False, writable=True, resolve_path=True, path_type=Path),
)
@option(
    "--siegfried-path",
    type=ClickPath(dir_okay=False, resolve_path=True, path_type=Path),
    default=None,
)
@option(
    "--siegfried-signature",
    type=Choice(("pronom", "loc", "tika", "freedesktop", "pronom-tika-loc", "deluxe", "archivematica")),
    default="pronom",
)
@pass_context
def app_process(ctx: Context, root: Path, siegfried_path: Optional[Path], siegfried_signature: TSignature):
    database_path: Path = root / "_metadata" / "files.db"
    database_path.parent.mkdir(exist_ok=True)

    siegfried = Siegfried(siegfried_path or Path(environ["GOPATH"], "bin", "sf"))
    siegfried.update(siegfried_signature)

    program_name: str = ctx.find_root().command.name
    logger: Logger = setup_logger(program_name, files=[database_path.parent / f"{program_name}.log"], streams=[stdout])
    logger_stdout: Logger = setup_logger(program_name + "_std_out", streams=[stdout])

    actions, custom_signatures = get_actions(), get_custom_signatures()

    with FileDB(database_path) as database, ExceptionManager(BaseException) as exception:
        database.init()
        database.commit()

        program_start: HistoryEntry = HistoryEntry.command_history(ctx, "start")

        database.history.insert(program_start)
        logger.info(program_start.operation)

        for path in find_files(root, exclude=[database_path.parent]):
            if database.file_exists(path, root):
                continue

            file_history: list[HistoryEntry] = []
            file = File.from_file(path, root, siegfried, actions, custom_signatures)

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
                        )
                    )

            database.files.insert(file, exist_ok=True)

            logger_stdout.info(
                f"{HistoryEntry.command_history(ctx, ':file:new').operation} "
                f"{file.relative_path} {file.puid} {file.action}"
            )

            for entry in file_history:
                logger.info(f"{entry.operation} {entry.uuid}")
                database.history.insert(entry)

        program_end: HistoryEntry = HistoryEntry.command_history(
            ctx,
            "end",
            data=1 if exception.exception else 0,
            reason="".join(format_tb(exception.traceback)) if exception.traceback else None,
        )
        if exception.exception:
            logger.error(f"{program_end.operation} {repr(exception.exception)}")
        else:
            logger.info(program_end.operation)

        if database.is_open:
            database.history.insert(program_end)
            database.commit()
