from logging import ERROR
from logging import INFO
from logging import Logger
from logging import WARNING
from typing import get_args as get_type_args
from typing import Type

from acacore.database import FilesDB
from acacore.models.event import Event
from acacore.models.file import OriginalFile
from acacore.models.reference_files import IgnoreAction
from acacore.models.reference_files import ManualAction
from acacore.siegfried.siegfried import TSignaturesProvider
from acacore.utils.click import end_program
from acacore.utils.click import start_program
from acacore.utils.functions import rm_tree
from acacore.utils.helpers import ExceptionManager
from click import Choice
from click import command
from click import Context
from click import option
from click import pass_context
from click import Path as ClickPath

from digiarch.__version__ import __version__
from digiarch.commands.identify import identify_original_file
from digiarch.commands.identify import identify_requirements
from digiarch.common import get_avid
from digiarch.common import open_database
from digiarch.common import option_dry_run
from digiarch.query import argument_query
from digiarch.query import query_to_where
from digiarch.query import TQuery

from .extractors.base import ExtractError
from .extractors.base import ExtractorBase
from .extractors.base import NotPreservableFileError
from .extractors.base import PasswordProtectedError
from .extractors.extractor_msg import MsgExtractor
from .extractors.extractor_patool import PatoolExtractor
from .extractors.extractor_tnef import TNEFExtractor
from .extractors.extractor_webarchive import WebarchiveExtractor
from .extractors.extractor_zip import ZipExtractor


def find_extractor(file: OriginalFile) -> tuple[Type[ExtractorBase] | None, str | None]:
    """
    Match an extractor class to a file.

    Matches the value at ``File.action_data.extract.tool`` to the ``ExtractorBase.tool_names`` of the various
    converters. The first matching extractor is returned. If multiple extractors support the same tool, then priority is
    given to the extractor matched first.
    :param file: The ``acacore.models.file.File`` object to find the extractor for.
    :return: An ``ExtractorBase`` class or ``None`` if an extractor could not be found, and the name of the tool.
    """
    if not file.action_data.extract:
        return None, None

    for extractor in (
        ZipExtractor,
        TNEFExtractor,
        MsgExtractor,
        PatoolExtractor,
        WebarchiveExtractor,
    ):
        if file.action_data.extract.tool in extractor.tool_names:
            return extractor, file.action_data.extract.tool

    return None, file.action_data.extract.tool


def next_archive_file(db: FilesDB, query: TQuery, offset: int = 0) -> OriginalFile | None:
    where, params = query_to_where(query)
    where = where or "action = 'extract'"
    return db.original_files.select(where, params, [("lower(relative_path)", "asc")], 1, offset).fetchone()


def handle_extract_error(
    ctx: Context,
    db: FilesDB,
    file: OriginalFile,
    err: ExtractError | None,
    *loggers: Logger,
):
    if not err:
        return
    elif isinstance(err, PasswordProtectedError):
        event = Event.from_command(ctx, "error", (file.uuid, "original"), err.__class__.__name__, err.msg)
        file.action = "ignore"
        file.action_data.ignore = IgnoreAction(template="password-protected")
        db.original_files.update(file)
        db.log.insert(event)
        event.log(ERROR, *loggers, show_args=["uuid"], error="password-protected", path=file.relative_path)
    elif isinstance(err, NotPreservableFileError):
        event = Event.from_command(ctx, "error", (file.uuid, "original"), err.__class__.__name__, err.msg)
        file.action = "ignore"
        file.action_data.ignore = IgnoreAction(template="not-preservable", reason=err.msg)
        db.original_files.update(file)
        db.log.insert(event)
        event.log(ERROR, *loggers, show_args=["uuid"], error="not-preservable", path=file.relative_path)
    else:
        event = Event.from_command(ctx, "error", file.uuid, err.__class__.__name__, err.msg)
        file.lock = True
        file.action = "manual"
        file.action_data.manual = ManualAction(reason=err.msg, process="")
        db.original_files.update(file)
        db.log.insert(event)
        event.log(ERROR, *loggers, show_args=["uuid"], error=repr(err), path=file.relative_path)


@command("extract", short_help="Unpack archives.")
@argument_query(False, "uuid", ["uuid", "checksum", "puid", "relative_path", "action", "warning", "processed", "lock"])
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
@option_dry_run()
@pass_context
def cmd_extract(
    ctx: Context,
    query: TQuery,
    siegfried_path: str | None,
    siegfried_signature: str,
    siegfried_home: str | None,
    actions_file: str | None,
    custom_signatures_file: str | None,
    dry_run: bool,
):
    """
    Unpack archives and identify files therein.

    Files are unpacked recursively, i.e., if an archive contains another archive, this will be unpacked as well.

    Archives with unrecognized extraction tools will be set to manual mode.

    To see the which files will be unpacked (but not their contents) without unpacking them, use the --dry-run option.
    """
    avid = get_avid(ctx)
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
        offset: int = 0

        with ExceptionManager(BaseException) as exception:
            while archive_file := next_archive_file(db, query, offset):
                archive_file.root = avid.path
                extractor_cls, extractor_tool = find_extractor(archive_file)

                if not extractor_cls:
                    Event.from_command(
                        ctx,
                        "skip",
                        (archive_file.uuid, "original"),
                        reason="Tool not found",
                    ).log(WARNING, log_stdout, tool=extractor_tool, path=archive_file.relative_path)
                    offset += 1
                    continue

                if dry_run:
                    Event.from_command(
                        ctx,
                        "unpacked",
                        (archive_file.uuid, "original"),
                    ).log(INFO, log_stdout, tool=extractor_tool, path=archive_file.relative_path)
                    offset += 1
                    continue

                extractor = extractor_cls(archive_file, avid.path)

                try:
                    extracted_files_paths = extractor.extract()
                    event = Event.from_command(
                        ctx,
                        "unpacked",
                        (archive_file.uuid, "original"),
                        len(extracted_files_paths),
                    )
                    event.log(INFO, log_stdout, files=len(extracted_files_paths), path=archive_file.relative_path)
                    db.log.insert(event)
                except ExtractError as err:
                    handle_extract_error(ctx, db, archive_file, err, log_stdout)
                    continue
                except Exception as err:
                    Event.from_command(ctx, "error", archive_file.uuid, None, repr(err)).log(
                        ERROR,
                        log_stdout,
                        show_args=["uuid"],
                        error=repr(err),
                        path=archive_file.relative_path,
                    )
                    raise
                finally:
                    if (folder := extractor.extract_folder).is_dir() and not next(folder.iterdir(), None):
                        rm_tree(folder)

                for path, original_path in extracted_files_paths:
                    identify_original_file(
                        ctx,
                        avid,
                        db,
                        siegfried.identify(path).files[0],
                        actions,
                        custom_signatures,
                        dry_run,
                        True,
                        archive_file.uuid,
                        original_path,
                        log_stdout,
                    )

                if archive_file.action_data.extract.on_success:
                    archive_file.action = archive_file.action_data.extract.on_success
                else:
                    archive_file.action = "ignore"
                    archive_file.action_data.ignore = IgnoreAction(template="extracted-archive")

                db.original_files.update(archive_file)

        end_program(ctx, db, exception, dry_run, log_file, log_stdout)