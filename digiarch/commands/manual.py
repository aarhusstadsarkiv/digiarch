from logging import INFO
from logging import WARNING
from pathlib import Path
from typing import Literal
from typing import Type
from uuid import UUID

from acacore.database.table import Table
from acacore.models.event import Event
from acacore.models.file import BaseFile
from acacore.models.file import ConvertedFile
from acacore.models.file import MasterFile
from acacore.models.file import OriginalFile
from acacore.utils.click import ctx_params
from acacore.utils.click import end_program
from acacore.utils.click import start_program
from acacore.utils.functions import find_files
from acacore.utils.helpers import ExceptionManager
from click import argument
from click import BadParameter
from click import Choice
from click import Context
from click import group
from click import option
from click import Parameter
from click import pass_context
from click import Path as ClickPath

from digiarch.__version__ import __version__
from digiarch.common import get_avid
from digiarch.common import open_database
from digiarch.common import option_dry_run


def callback_uuid(ctx: Context, param: Parameter, value: str | None) -> UUID | None:
    if value is None:
        return value

    try:
        return UUID(value)
    except ValueError as err:
        raise BadParameter(err.args[0] if err.args else "Invalid UUID.", ctx, param)


@group("manual", no_args_is_help=True, short_help="Perform actions manually.")
def grp_manual():
    """Perform complex actions manually when the automated tools fail or when one is not available."""


@grp_manual.command("extract", no_args_is_help=True, short_help="Add extracted files.")
@argument("parent", type=str, callback=callback_uuid, required=True)
@argument(
    "files",
    metavar="FILE...",
    type=ClickPath(exists=True, readable=True, resolve_path=True),
    nargs=-1,
    required=True,
)
@option("--exclude", type=str, multiple=True, help="File and folder names to exclude.  [multiple]")
@option_dry_run()
@pass_context
def cmd_manual_extract(
    ctx: Context,
    parent: UUID,
    files: tuple[str | Path, ...],
    exclude: tuple[str, ...],
    dry_run: bool,
):
    """
    Manually add files extracted from an archive, and assign them the PARENT UUID.

    The given FILEs can be single files or folders and must be located inside OriginalDocuments. All of them will be
    interpreted as direct children of the PARENT file, so archive files should be left unextracted for further
    processing with either extract or manual extract.

    To exclude children files when using a folder as target, use the --exclude option.

    If the files are not already in the database they will be added without identification.
    Run the identify original command to assign them a PUID and action.

    If the files are in the database their parent value will be set to ORIGINAL unless they already have one
    assigned, in which case they will be ignored.
    Run the identify command to assign a PUID and action to newly-added files.

    To see the changes without committing them, use the --dry-run option.
    """
    avid = get_avid(ctx)
    files = tuple(map(Path, files))

    if any(f == avid.dirs.original_documents or not f.is_relative_to(avid.dirs.original_documents) for f in files):
        raise BadParameter("Files not in OriginalDocuments.", ctx, ctx_params(ctx)["files"])

    with open_database(ctx, avid) as database:
        log_file, log_stdout, _ = start_program(ctx, database, __version__, None, True, True, dry_run)

        with ExceptionManager() as exception:
            parent_file = database.original_files[{"uuid": str(parent)}]

            if not parent_file:
                raise FileNotFoundError(f"No original file with UUID {parent}.")

            file_paths = (p for f in files for p in find_files(f, exclude=[avid.dirs.original_documents / "_metadata"]))

            for path in file_paths:
                if exclude and any(p in exclude for p in path.parts):
                    continue

                file = database.original_files[{"relative_path": str(path.relative_to(avid.path))}]
                exists: bool = file is not None

                if file and file.parent:
                    Event.from_command(ctx, "skip", (file.uuid, "original")).log(
                        WARNING,
                        log_stdout,
                        path=file.relative_path,
                        parent=file.parent,
                        reason="File already has a parent.",
                    )
                    continue
                elif not file:  # noqa: RET507
                    file = OriginalFile.from_file(path, avid.path)

                event = Event.from_command(
                    ctx,
                    "edit" if exists else "new",
                    (file.uuid, "original"),
                    {"parent": parent_file.uuid},
                )

                if not dry_run:
                    file.parent = parent_file.uuid
                    database.original_files.insert(file, on_exists="replace")
                    database.log.insert(event)

                event.log(INFO, log_stdout, show_args=["uuid"], path=file.relative_path)

        end_program(ctx, database, exception, dry_run, log_file, log_stdout)


@grp_manual.command("convert", no_args_is_help=True, short_help="Add converted files.")
@argument("original", type=str, callback=callback_uuid, required=True)
@argument("target", type=Choice(["master", "access", "statutory"]), required=True)
@argument(
    "files",
    metavar="FILE...",
    type=ClickPath(exists=True, dir_okay=False, readable=True, resolve_path=True),
    nargs=-1,
    required=True,
)
@option_dry_run()
@pass_context
def cmd_manual_convert(
    ctx: Context,
    original: UUID,
    target: Literal["master", "access", "statutory"],
    files: tuple[str | Path, ...],
    dry_run: bool,
):
    """
    Manually add converted files with ORIGINAL UUID as their parent.

    \b
    Depending on the TARGET, a different type of ORIGINAL file will be needed:
    * "master": original file parent
    * "access": master file parent
    * "statutory": master file parent

    The given FILEs must be located inside the MasterDocuments, AccessDocuments, or Documents folder depending on the
    TARGET.

    If the files are already in the database they will be ignored.
    Run the identify command to assign a PUID (and action where applicable) to newly-added files.

    To see the changes without committing them, use the --dry-run option.
    """  # noqa: D301
    avid = get_avid(ctx)

    target_dir: Path

    if target == "master":
        target_dir = avid.dirs.master_documents
    elif target == "access":
        target_dir = avid.dirs.access_documents
    elif target == "statutory":
        target_dir = avid.dirs.documents
    else:
        raise BadParameter("Invalid target.", ctx, ctx_params(ctx)["target"])

    if any(Path(f) == target_dir or not Path(f).is_relative_to(target_dir) for f in files):
        raise BadParameter(f"Files not in {target_dir.name}.", ctx, ctx_params(ctx)["files"])

    with open_database(ctx, avid) as database:
        log_file, log_stdout, _ = start_program(ctx, database, __version__, None, True, True, dry_run)
        parent_table: Table[BaseFile]
        target_table: Table[ConvertedFile]
        target_class: Type[ConvertedFile]

        if target == "master":
            parent_table = database.original_files
            target_table = database.master_files
            target_class = MasterFile
        elif target == "access":
            parent_table = database.master_files
            target_table = database.access_files
            target_class = ConvertedFile
        elif target == "statutory":
            parent_table = database.master_files
            target_table = database.statutory_files
            target_class = ConvertedFile

        with ExceptionManager() as exception:
            parent_file = parent_table[{"uuid": str(original)}]

            if not parent_file:
                raise FileNotFoundError(f"No file with UUID {original}.")

            for path in map(Path, files):
                if file_db := target_table[{"relative_path": str(path.relative_to(avid.path))}]:
                    if file_db.original_uuid != parent_file.uuid:
                        Event.from_command(ctx, "skip", (file_db.uuid, target)).log(
                            WARNING,
                            log_stdout,
                            path=file_db.relative_path,
                            reason="File already exists.",
                        )
                    continue

                file = target_class.from_file(path, avid.path, parent_file.uuid)
                event = Event.from_command(ctx, "new", (file.uuid, target), {"original_uuid": parent_file.uuid})

                if not dry_run:
                    target_table.insert(file)
                    database.log.insert(event)

                event.log(INFO, log_stdout, show_args=["uuid"], path=file.relative_path)

        end_program(ctx, database, exception, dry_run, log_file, log_stdout)
