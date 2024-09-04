from logging import INFO
from logging import Logger
from pathlib import Path
from typing import Any

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
from click import BadParameter
from click import Choice
from click import Context
from click import group
from click import MissingParameter
from click import option
from click import pass_context
from click import Path as ClickPath
from pydantic import BaseModel

from digiarch.common import argument_root
from digiarch.common import check_database_version
from digiarch.common import ctx_params
from digiarch.common import docstring_format
from digiarch.common import end_program
from digiarch.common import fetch_actions
from digiarch.common import option_dry_run
from digiarch.common import param_regex
from digiarch.common import start_program

from .common import argument_query
from .common import find_files
from .common import TQuery


def set_lock(
    ctx: Context,
    database: FileDB,
    file: File,
    reason: str,
    dry_run: bool,
    *loggers: Logger,
):
    if file.lock is True:
        return
    event = HistoryEntry.command_history(ctx, "lock", file.uuid, [file.lock, True], reason)
    file.lock = True
    if not dry_run:
        database.files.update(file, {"uuid": file.uuid})
        database.history.insert(event)
    event.log(INFO, *loggers)


def set_action(
    ctx: Context,
    database: FileDB,
    file: File,
    action: TActionType,
    action_data: BaseModel | dict,
    reason: str,
    dry_run: bool,
    *loggers: Logger,
):
    if not isinstance(action_data, dict):
        action_data = action_data.model_dump()
    if file.action == action and file.action_data.model_dump().get(action) == action_data:
        HistoryEntry.command_history(ctx, "skip", file.uuid, None, "No changes").log(*loggers)
        return
    old_action: dict[TActionType, dict | None] = {action: file.action_data.model_dump().get(action)}
    new_action: dict[TActionType, dict] = {action: action_data}
    event = HistoryEntry.command_history(ctx, "edit", file.uuid, [file.action, action, old_action, new_action], reason)
    file.action = action
    file.action_data = ActionData.model_validate(file.action_data.model_dump() | new_action)
    if not dry_run:
        database.files.update(file, {"uuid": file.uuid})
        database.history.insert(event)
    event.log(INFO, *loggers)


@group("action")
def group_action():
    """Change file actions."""


@group_action.command("convert", no_args_is_help=True, short_help="Set convert action.")
@argument_root(True)
@argument_query(True, "uuid", ["uuid", "checksum", "puid", "relative_path", "action", "warning", "processed", "lock"])
@argument("reason", nargs=1, type=str, required=True)
@option("--tool", type=str, required=True, help="The tool to use for conversion.")
@option(
    "--outputs",
    type=str,
    multiple=True,
    callback=param_regex("^(.[a-zA-Z0-9]+)+$"),
    help='The file extensions to generate.  [multiple; required for tools other than "copy"]',
)
@option("--lock", is_flag=True, default=False, help="Lock the edited files.")
@option_dry_run()
@pass_context
def action_convert(
    ctx: Context,
    root: Path,
    reason: str,
    query: TQuery,
    tool: str,
    outputs: tuple[str, ...],
    lock: bool,
    dry_run: bool,
):
    """
    Set files' action to "convert".

    The --outputs option may be omitted when using the "copy" tool.

    To lock the file(s) after editing them, use the --lock option.

    To see the changes without committing them, use the --dry-run option.

    For details on the QUERY argument, see the edit command.
    """
    if tool not in ("copy",) and not outputs:
        raise MissingParameter(f"Required for tool {tool!r}.", ctx, ctx_params(ctx)["outputs"])

    data = ConvertAction(tool=tool, outputs=outputs)

    check_database_version(ctx, ctx_params(ctx)["root"], (db_path := root / "_metadata" / "files.db"))

    with FileDB(db_path) as database:
        log_file, log_stdout, _ = start_program(ctx, database, None, True, True, dry_run)

        with ExceptionManager(BaseException) as exception:
            for file in find_files(database, query):
                set_action(ctx, database, file, "convert", data, reason, dry_run, log_stdout)
                if lock:
                    set_lock(ctx, database, file, reason, dry_run, log_stdout)

        end_program(ctx, database, exception, dry_run, log_file, log_stdout)


@group_action.command("extract", no_args_is_help=True, short_help="Set extract action.")
@argument_root(True)
@argument_query(True, "uuid", ["uuid", "checksum", "puid", "relative_path", "action", "warning", "processed", "lock"])
@argument("reason", nargs=1, type=str, required=True)
@option("--tool", type=str, required=True, help="The tool to use for extraction.")
@option(
    "--extension",
    type=str,
    callback=param_regex(r"^(.[a-zA-Z0-9]+)+$"),
    help="The extension the file must have for extraction to succeed.",
)
@option("--lock", is_flag=True, default=False, help="Lock the edited files.")
@option_dry_run()
@pass_context
def action_extract(
    ctx: Context,
    root: Path,
    reason: str,
    query: TQuery,
    tool: str,
    extension: str | None,
    lock: bool,
    dry_run: bool,
):
    """
    Set files' action to "extract".

    To lock the file(s) after editing them, use the --lock option.

    To see the changes without committing them, use the --dry-run option.

    For details on the QUERY argument, see the edit command.
    """
    data = ExtractAction(tool=tool, extension=extension)

    check_database_version(ctx, ctx_params(ctx)["root"], (db_path := root / "_metadata" / "files.db"))

    with FileDB(db_path) as database:
        log_file, log_stdout, _ = start_program(ctx, database, None, True, True, dry_run)

        with ExceptionManager(BaseException) as exception:
            for file in find_files(database, query):
                set_action(ctx, database, file, "extract", data, reason, dry_run, log_stdout)
                if lock:
                    set_lock(ctx, database, file, reason, dry_run, log_stdout)

        end_program(ctx, database, exception, dry_run, log_file, log_stdout)


@group_action.command("manual", no_args_is_help=True, short_help="Set manual action.")
@argument_root(True)
@argument_query(True, "uuid", ["uuid", "checksum", "puid", "relative_path", "action", "warning", "processed", "lock"])
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
@option("--lock", is_flag=True, default=False, help="Lock the edited files.")
@option_dry_run()
@pass_context
def action_manual(
    ctx: Context,
    root: Path,
    reason: str,
    query: TQuery,
    data_reason: str | None,
    process: str,
    lock: bool,
    dry_run: bool,
):
    """
    Set files' action to "manual".

    To lock the file(s) after editing them, use the --lock option.

    To see the changes without committing them, use the --dry-run option.

    For details on the QUERY argument, see the edit command.
    """
    data = ManualAction(reason=data_reason, process=process)

    check_database_version(ctx, ctx_params(ctx)["root"], (db_path := root / "_metadata" / "files.db"))

    with FileDB(db_path) as database:
        log_file, log_stdout, _ = start_program(ctx, database, None, True, True, dry_run)

        with ExceptionManager(BaseException) as exception:
            for file in find_files(database, query):
                set_action(ctx, database, file, "manual", data, reason, dry_run, log_stdout)
                if lock:
                    set_lock(ctx, database, file, reason, dry_run, log_stdout)

        end_program(ctx, database, exception, dry_run, log_file, log_stdout)


@group_action.command("ignore", no_args_is_help=True, short_help="Set ignore action.")
@argument_root(True)
@argument_query(True, "uuid", ["uuid", "checksum", "puid", "relative_path", "action", "warning", "processed", "lock"])
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
@option("--lock", is_flag=True, default=False, help="Lock the edited files.")
@option_dry_run()
@pass_context
@docstring_format(templates="\n".join(f"    * {t}" for t in TemplateTypeEnum).strip())
def action_ignore(
    ctx: Context,
    root: Path,
    reason: str,
    query: TQuery,
    template: TTemplateType,
    data_reason: str | None,
    lock: bool,
    dry_run: bool,
):
    """
    Set files' action to "ignore".

    \b
    Template must be one of:
    {templates}

    The --reason option may be omitted when using a template other than "text".

    To lock the file(s) after editing them, use the --lock option.

    To see the changes without committing them, use the --dry-run option.

    For details on the QUERY argument, see the edit command.
    """  # noqa: D301
    if template in ("text",) and not data_reason:
        raise MissingParameter(f"Required for template {template!r}.", ctx, ctx_params(ctx)["data_reason"])

    data = IgnoreAction(template=template, reason=data_reason)

    check_database_version(ctx, ctx_params(ctx)["root"], (db_path := root / "_metadata" / "files.db"))

    with FileDB(db_path) as database:
        log_file, log_stdout, _ = start_program(ctx, database, None, True, True, dry_run)

        with ExceptionManager(BaseException) as exception:
            for file in find_files(database, query):
                set_action(ctx, database, file, "ignore", data, reason, dry_run, log_stdout)
                if lock:
                    set_lock(ctx, database, file, reason, dry_run, log_stdout)

        end_program(ctx, database, exception, dry_run, log_file, log_stdout)


@group_action.command("copy", no_args_is_help=True, short_help="Copy action from a format.")
@argument_root(True)
@argument_query(True, "uuid", ["uuid", "checksum", "puid", "relative_path", "action", "warning", "processed", "lock"])
@argument("puid", nargs=1, type=str, required=True)
@argument("action", type=Choice(["convert", "extract", "manual", "ignore"]))
@argument("reason", nargs=1, type=str, required=True)
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
@option("--lock", is_flag=True, default=False, help="Lock the edited files.")
@option_dry_run()
@pass_context
def command_copy(
    ctx: Context,
    root: Path,
    puid: str,
    action: TActionType,
    reason: str,
    query: TQuery,
    actions_file: Path | None,
    lock: bool,
    dry_run: bool,
):
    """
    Set files' action by copying it from an existing format.

    \b
    Supported actions are:
    * convert
    * extract
    * manual
    * ignore

    If no actions file is give with --actions, the latest version will be downloaded from GitHub.

    To lock the file(s) after editing them, use the --lock option.

    To see the changes without committing them, use the --dry-run option.

    For details on the QUERY argument, see the edit command.
    """  # noqa: D301
    check_database_version(ctx, ctx_params(ctx)["root"], (db_path := root / "_metadata" / "files.db"))

    actions = fetch_actions(ctx, "actions_file", actions_file)

    if not (action_model := actions.get(puid)):
        raise BadParameter(f"Format {puid} not found.", ctx, ctx_params(ctx)["puid"])

    action_data: dict[str, Any] = action_model.action_data.model_dump()

    if not (data := action_data.get(action)):
        raise BadParameter(f"Action {action} not found in {puid}.", ctx, ctx_params(ctx)["puid"])

    with FileDB(db_path) as database:
        log_file, log_stdout, _ = start_program(ctx, database, None, True, True, dry_run)

        with ExceptionManager(BaseException) as exception:
            for file in find_files(database, query):
                set_action(ctx, database, file, action, data, reason, dry_run, log_stdout)
                if lock:
                    set_lock(ctx, database, file, reason, dry_run, log_stdout)

        end_program(ctx, database, exception, dry_run, log_file, log_stdout)


group_action.list_commands = lambda _ctx: list(group_action.commands)
