from logging import ERROR
from logging import INFO
from logging import WARNING
from os import environ
from pathlib import Path
from typing import get_args as get_type_args
from typing import Type

from acacore.database import FileDB
from acacore.exceptions.files import IdentificationError
from acacore.models.file import File
from acacore.models.history import HistoryEntry
from acacore.models.reference_files import IgnoreAction
from acacore.models.reference_files import ManualAction
from acacore.siegfried import Siegfried
from acacore.siegfried.siegfried import TSignaturesProvider
from acacore.utils.helpers import ExceptionManager
from click import BadParameter
from click import Choice
from click import command
from click import Context
from click import option
from click import pass_context
from click import Path as ClickPath

from digiarch.common import argument_root
from digiarch.common import check_database_version
from digiarch.common import ctx_params
from digiarch.common import end_program
from digiarch.common import fetch_actions
from digiarch.common import fetch_custom_signatures
from digiarch.common import option_dry_run
from digiarch.common import start_program
from digiarch.identify import identify_file

from .extractors.base import ExtractError
from .extractors.base import ExtractorBase
from .extractors.base import NotPreservableFileError
from .extractors.base import PasswordProtectedError
from .extractors.extractor_msg import MsgExtractor
from .extractors.extractor_patool import PatoolExtractor
from .extractors.extractor_tnef import TNEFExtractor
from .extractors.extractor_zip import ZipExtractor


def find_extractor(file: File) -> tuple[Type[ExtractorBase] | None, str | None]:
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

    for extractor in (ZipExtractor, TNEFExtractor, MsgExtractor, PatoolExtractor):
        if file.action_data.extract.tool in extractor.tool_names:
            return extractor, file.action_data.extract.tool

    return None, file.action_data.extract.tool


@command("extract", no_args_is_help=True, short_help="Unpack archives.")
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
@option_dry_run()
@pass_context
def command_extract(
    ctx: Context,
    root: Path,
    siegfried_path: Path,
    siegfried_home: Path,
    siegfried_signature: TSignaturesProvider,
    actions_file: Path | None,
    custom_signatures_file: Path | None,
    dry_run: bool,
):
    """
    Unpack archives and identify files therein.

    Files are unpacked recursively, i.e., if an archive contains another archive, this will be unpacked as well.

    Archives with unrecognized extraction tools will be set to manual mode.

    To see the which files will be unpacked (but not their contents) without unpacking them, use the --dry-run option.
    """
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

    with FileDB(db_path) as database:
        log_file, log_stdout, _ = start_program(ctx, database, None, True, True, dry_run)
        offset: int = 0

        with ExceptionManager(BaseException) as exception:
            while archive_file := database.files.select(
                where="action = 'extract' and not processed",
                order_by=[("lower(relative_path)", "asc")],
                offset=offset,
                limit=1,
            ).fetchone():
                extractor_cls, extractor_tool = find_extractor(archive_file)
                if not extractor_cls:
                    HistoryEntry.command_history(
                        ctx,
                        "skip",
                        archive_file.uuid,
                        extractor_tool,
                        "Tool not found",
                    ).log(WARNING, log_stdout, path=archive_file.relative_path)
                    offset += 1
                    continue

                if dry_run:
                    HistoryEntry.command_history(ctx, "unpacked", archive_file.uuid).log(
                        INFO, log_stdout, tool=extractor_tool, path=archive_file.relative_path
                    )
                    offset += 1
                    continue

                # noinspection PyBroadException
                try:
                    extractor = extractor_cls(database, archive_file, root)
                    extracted_files_paths = list(extractor.extract())
                    event = HistoryEntry.command_history(ctx, "unpacked", archive_file.uuid)
                    event.log(INFO, log_stdout, path=archive_file.relative_path)
                    database.history.insert(event)
                except PasswordProtectedError as err:
                    event = HistoryEntry.command_history(
                        ctx,
                        "error",
                        archive_file.uuid,
                        err.__class__.__name__,
                        err.msg,
                    )
                    archive_file.action = "ignore"
                    archive_file.action_data.ignore = IgnoreAction(template="password-protected")
                    archive_file.processed = True
                    database.files.update(archive_file)
                    database.history.insert(event)
                    event.log(ERROR, log_file, log_stdout, path=archive_file.relative_path)
                    continue
                except NotPreservableFileError as err:
                    event = HistoryEntry.command_history(
                        ctx,
                        "skip",
                        archive_file.uuid,
                        err.__class__.__name__,
                        err.msg,
                    )
                    archive_file.action = "ignore"
                    archive_file.action_data.ignore = IgnoreAction(template="not-preservable")
                    archive_file.processed = True
                    database.files.update(archive_file)
                    database.history.insert(event)
                    event.log(ERROR, log_file, log_stdout, path=archive_file.relative_path)
                    continue
                except ExtractError as err:
                    event = HistoryEntry.command_history(
                        ctx,
                        "error",
                        archive_file.uuid,
                        err.__class__.__name__,
                        err.msg,
                    )
                    archive_file.action = "manual"
                    archive_file.action_data.manual = ManualAction(reason=err.msg, process="")
                    database.files.update(archive_file)
                    database.history.insert(event)
                    event.log(ERROR, log_file, log_stdout)
                    continue
                except Exception as err:
                    event = HistoryEntry.command_history(ctx, "error", archive_file.uuid, None, repr(err))
                    event.log(ERROR, log_file, log_stdout)
                    raise

                for path, original_path in extracted_files_paths:
                    extracted_file, file_history = identify_file(
                        ctx,
                        root,
                        path,
                        database,
                        siegfried,
                        None,
                        actions,
                        custom_signatures,
                        parent=archive_file.uuid,
                    )
                    if extracted_file:
                        event = HistoryEntry.command_history(
                            ctx,
                            "new",
                            extracted_file.uuid,
                        )
                        event.log(
                            INFO,
                            log_stdout,
                            puid=extracted_file.puid,
                            action=extracted_file.action,
                            path=extracted_file.relative_path,
                        )
                        database.history.insert(event)
                        if path != original_path:
                            file_history.insert(
                                0,
                                HistoryEntry.command_history(
                                    ctx,
                                    "rename",
                                    extracted_file.uuid,
                                    [str(original_path.relative_to(root)), str(path.relative_to(root))],
                                ),
                            )
                        for event in file_history:
                            event.log(INFO, log_stdout)
                            database.history.insert(event)
                    else:
                        HistoryEntry.command_history(ctx, "skip", reason="File already exists").log(
                            WARNING, log_stdout, path=path
                        )
                        for event in file_history:
                            event.log(WARNING, log_stdout)

                if archive_file.action_data.extract.on_success:
                    archive_file.action = archive_file.action_data.extract.on_success
                else:
                    archive_file.action = "ignore"
                    archive_file.action_data.ignore = IgnoreAction(template="extracted-archive")
                    archive_file.processed = True
                database.files.update(archive_file)

        end_program(ctx, database, exception, dry_run, log_file, log_stdout)
