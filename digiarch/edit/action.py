from logging import INFO
from logging import Logger
from pathlib import Path

from acacore.database import FileDB
from acacore.models.file import File
from acacore.models.history import HistoryEntry
from acacore.models.reference_files import ActionData
from acacore.models.reference_files import ConvertAction
from acacore.models.reference_files import ExtractAction
from acacore.models.reference_files import IgnoreAction
from acacore.models.reference_files import ManualAction
from acacore.models.reference_files import TActionType
from acacore.models.reference_files import TemplateTypeEnum
from acacore.models.reference_files import TTemplateType
from acacore.utils.helpers import ExceptionManager
from click import argument
from click import Choice
from click import Context
from click import group
from click import MissingParameter
from click import option
from click import pass_context
from pydantic import BaseModel

from digiarch.common import argument_root
from digiarch.common import check_database_version
from digiarch.common import ctx_params
from digiarch.common import end_program
from digiarch.common import option_dry_run
from digiarch.common import param_regex
from digiarch.common import start_program

from .common import argument_ids
from .common import find_files


def set_action(
    ctx: Context,
    database: FileDB,
    file: File,
    action: TActionType,
    action_data: BaseModel,
    reason: str,
    *loggers: Logger,
):
    if file.action == action and file.action_data.model_dump().get(action) == action_data.model_dump():
        HistoryEntry.command_history(ctx, "skip", file.uuid, None, "No changes").log(*loggers)
        return
    old_action: dict[TActionType, dict | None] = {action: file.action_data.model_dump().get(action)}
    new_action: dict[TActionType, dict] = {action: action_data.model_dump()}
    event = HistoryEntry.command_history(ctx, "edit", file.uuid, [file.action, action, old_action, new_action], reason)
    file.action = action
    file.action_data = ActionData.model_validate(file.action_data.model_dump() | new_action)
    database.files.update(file, {"uuid": file.uuid})
    database.history.insert(event)
    event.log(INFO, *loggers)


@group("action")
def group_action():
    """Change the action of one or more files."""


@group_action.command("convert", no_args_is_help=True, short_help="Set convert action.")
@argument_root(True)
@argument_ids(True)
@argument("reason", nargs=1, type=str, required=True)
@option("--tool", type=str, required=True, help="The tool to use for conversion.")
@option(
    "--outputs",
    type=str,
    multiple=True,
    callback=param_regex("^(.[a-zA-Z0-9]+)+$"),
    help='The file extensions to generate.  [multiple; required for tools other than "copy"]',
)
@option_dry_run()
@pass_context
def action_convert(
    ctx: Context,
    root: Path,
    reason: str,
    ids: tuple[str, ...],
    id_type: str,
    id_files: bool,
    tool: str,
    outputs: tuple[str, ...],
    dry_run: bool,
):
    """
    Set files' action to "convert".

    The --outputs option may be omitted when using the "copy" tool.
    """
    if tool not in ("copy",) and not outputs:
        raise MissingParameter(f"Required for tool {tool!r}.", ctx, ctx_params(ctx)["outputs"])

    data = ConvertAction(tool=tool, outputs=outputs)

    check_database_version(ctx, ctx_params(ctx)["root"], (db_path := root / "_metadata" / "files.db"))

    with FileDB(db_path) as database:
        log_file, log_stdout = start_program(ctx, database, None, True, True, dry_run)

        with ExceptionManager(BaseException) as exception:
            for file in find_files(database, ids, id_type, id_files):
                set_action(ctx, database, file, "convert", data, reason, log_stdout)

        end_program(ctx, database, exception, dry_run, log_file, log_stdout)


@group_action.command("extract", no_args_is_help=True, short_help="Set extract action.")
@argument_root(True)
@argument_ids(True)
@argument("reason", nargs=1, type=str, required=True)
@option("--tool", type=str, required=True, help="The tool to use for extraction.")
@option(
    "--extension",
    type=str,
    callback=param_regex(r"^(.[a-zA-Z0-9]+)+$"),
    help="The extension the file must have for extraction to succeed.",
)
@option_dry_run()
@pass_context
def action_extract(
    ctx: Context,
    root: Path,
    reason: str,
    ids: tuple[str, ...],
    id_type: str,
    id_files: bool,
    tool: str,
    extension: str | None,
    dry_run: bool,
):
    """Set files' action to "extract"."""
    data = ExtractAction(tool=tool, extension=extension)

    check_database_version(ctx, ctx_params(ctx)["root"], (db_path := root / "_metadata" / "files.db"))

    with FileDB(db_path) as database:
        log_file, log_stdout = start_program(ctx, database, None, True, True, dry_run)

        with ExceptionManager(BaseException) as exception:
            for file in find_files(database, ids, id_type, id_files):
                set_action(ctx, database, file, "extract", data, reason, log_stdout)

        end_program(ctx, database, exception, dry_run, log_file, log_stdout)


@group_action.command("manual", no_args_is_help=True, short_help="Set manual action.")
@argument_root(True)
@argument_ids(True)
@argument("reason", nargs=1, type=str, required=True)
@option(
    "--reason",
    "data_reason",
    type=str,
    required=True,
    callback=param_regex(r"^.*\S.*$"),
    help="The reason why the file must be processed manually.",
)
@option(
    "--process",
    type=str,
    required=True,
    callback=param_regex(r"^.*\S.*$"),
    help="The steps to take to process the file.",
)
@option_dry_run()
@pass_context
def action_manual(
    ctx: Context,
    root: Path,
    reason: str,
    ids: tuple[str, ...],
    id_type: str,
    id_files: bool,
    data_reason: str | None,
    process: str,
    dry_run: bool,
):
    """Set files' action to "manual"."""
    data = ManualAction(reason=data_reason, process=process)

    check_database_version(ctx, ctx_params(ctx)["root"], (db_path := root / "_metadata" / "files.db"))

    with FileDB(db_path) as database:
        log_file, log_stdout = start_program(ctx, database, None, True, True, dry_run)

        with ExceptionManager(BaseException) as exception:
            for file in find_files(database, ids, id_type, id_files):
                set_action(ctx, database, file, "manual", data, reason, log_stdout)

        end_program(ctx, database, exception, dry_run, log_file, log_stdout)


@group_action.command("ignore", no_args_is_help=True, short_help="Set ignore action.")
@argument_root(True)
@argument_ids(True)
@argument("reason", nargs=1, type=str, required=True)
@option(
    "--template",
    metavar="TEMPLATE",
    type=Choice(TemplateTypeEnum, False),
    required=True,
    help="The template type to use.",
)
@option(
    "--reason",
    "data_reason",
    type=str,
    callback=param_regex(r"^.*\S.*$"),
    help='The reason why the file is ignored.  [required for "text" template]',
)
@option_dry_run()
@pass_context
def action_ignore(
    ctx: Context,
    root: Path,
    reason: str,
    ids: tuple[str, ...],
    id_type: str,
    id_files: bool,
    template: TTemplateType,
    data_reason: str | None,
    dry_run: bool,
):
    """
    Set files' action to "ignore".

    \b
    Template must be one of:
    * text
    * empty
    * password-protected
    * corrupted
    * duplicate
    * not-preservable
    * not-convertable

    The --reason option may be omitted when using a template other than "text".
    """
    if template in ("text",) and not data_reason:
        raise MissingParameter(f"Required for template {template!r}.", ctx, ctx_params(ctx)["data_reason"])

    data = IgnoreAction(template=template, reason=data_reason)

    check_database_version(ctx, ctx_params(ctx)["root"], (db_path := root / "_metadata" / "files.db"))

    with FileDB(db_path) as database:
        log_file, log_stdout = start_program(ctx, database, None, True, True, dry_run)

        with ExceptionManager(BaseException) as exception:
            for file in find_files(database, ids, id_type, id_files):
                set_action(ctx, database, file, "ignore", data, reason, log_stdout)

        end_program(ctx, database, exception, dry_run, log_file, log_stdout)
