from logging import INFO
from logging import Logger
from typing import Any
from typing import Literal

from acacore.database import FilesDB
from acacore.models.event import Event
from acacore.models.file import MasterFile
from acacore.models.file import OriginalFile
from acacore.models.reference_files import ActionData
from acacore.models.reference_files import ConvertAction
from acacore.models.reference_files import ExtractAction
from acacore.models.reference_files import IgnoreAction
from acacore.models.reference_files import ManualAction
from acacore.models.reference_files import TActionType
from acacore.models.reference_files import TemplateTypeEnum
from acacore.models.reference_files import TTemplateType
from acacore.utils.click import end_program
from acacore.utils.click import param_callback_regex
from acacore.utils.click import start_program
from acacore.utils.decorators import docstring_format
from acacore.utils.helpers import ExceptionManager
from click import argument
from click import BadParameter
from click import Choice
from click import command
from click import Context
from click import group
from click import MissingParameter
from click import option
from click import pass_context
from click import Path as ClickPath
from pydantic import BaseModel

from digiarch.__version__ import __version__
from digiarch.common import AVID
from digiarch.common import CommandWithRollback
from digiarch.common import ctx_params
from digiarch.common import fetch_actions
from digiarch.common import get_avid
from digiarch.common import open_database
from digiarch.common import option_dry_run
from digiarch.common import rollback
from digiarch.query import argument_query
from digiarch.query import query_table
from digiarch.query import TQuery


def set_lock(
    ctx: Context,
    database: FilesDB,
    file: OriginalFile,
    reason: str,
    dry_run: bool,
    *loggers: Logger,
):
    if file.lock is True:
        return
    event = Event.from_command(ctx, "lock", (file.uuid, "original"), [file.lock, True], reason)
    file.lock = True
    if not dry_run:
        database.original_files.update(file)
        database.log.insert(event)
    event.log(INFO, *loggers, show_args=["uuid", "data"])


def set_action(
    ctx: Context,
    database: FilesDB,
    file: OriginalFile,
    action: TActionType,
    action_data: BaseModel | dict,
    reason: str,
    dry_run: bool,
    *loggers: Logger,
):
    if not isinstance(action_data, dict):
        action_data = action_data.model_dump()
    if file.action == action and file.action_data.model_dump().get(action) == action_data:
        Event.from_command(ctx, "skip", (file.uuid, "original"), [file.action, action], "No changes").log(
            INFO,
            *loggers,
            show_args=["uuid", "data"],
        )
        return
    old_action: dict[TActionType, dict | None] = {action: file.action_data.model_dump().get(action)}
    new_action: dict[TActionType, dict] = {action: action_data}
    event = Event.from_command(
        ctx,
        "edit",
        (file.uuid, "original"),
        [file.action, action, old_action, new_action],
        reason,
    )
    file.action = action
    file.action_data = ActionData.model_validate(file.action_data.model_dump() | new_action)
    if not dry_run:
        database.original_files.update(file, {"uuid": str(file.uuid)})
        database.log.insert(event)
    event.log(INFO, *loggers, show_args=["uuid", "data"])


def rollback_set_action(_ctx: Context, _avid: AVID, database: FilesDB, event: Event, file: OriginalFile | None):
    if not file:
        return

    prev_action: TActionType | None
    prev_action_data: dict[TActionType, dict | None]
    prev_action, _, prev_action_data, _ = event.data

    file.action = prev_action
    file.action_data = ActionData.model_validate(file.action_data.model_dump(mode="json") | prev_action_data)

    database.original_files.update(file)


def set_master_convert(
    ctx: Context,
    database: FilesDB,
    file: MasterFile,
    action: ConvertAction,
    action_type: Literal["access", "statutory"],
    reason: str,
    dry_run: bool,
    *loggers: Logger,
):
    if action_type == "access" and file.convert_access.model_dump() == action:
        Event.from_command(ctx, "skip", (file.uuid, "master")).log(INFO, *loggers, show_args=["uuid"])
        return
    if action_type == "statutory" and file.convert_statutory.model_dump() == action:
        Event.from_command(ctx, "skip", (file.uuid, "master")).log(INFO, *loggers, show_args=["uuid"])
        return
    old_action: ConvertAction | None
    if action_type == "access":
        old_action = file.convert_access
        file.convert_access = action
    elif action_type == "statutory":
        old_action = file.convert_statutory
        file.convert_statutory = action
    else:
        return
    event = Event.from_command(
        ctx,
        "edit",
        (file.uuid, "master"),
        [action_type, old_action, action],
        reason,
    )
    if not dry_run:
        database.master_files.update(file)
        database.log.insert(event)
    event.log(INFO, *loggers, show_args=["uuid", "data"])


def rollback_set_master_convert(_ctx: Context, _avid: AVID, database: FilesDB, event: Event, file: MasterFile | None):
    if not file:
        return

    action_type: Literal["access", "statutory"]
    prev_action: ConvertAction | None
    action_type, prev_action, _ = event.data

    if action_type == "access":
        file.convert_access = prev_action
    elif action_type == "statutory":
        file.convert_statutory = prev_action

    database.master_files.update(file)


@group("action")
def grp_action_original():
    """Change actions of original files."""


@rollback("edit", rollback_set_action)
@grp_action_original.command("convert", no_args_is_help=True, short_help="Set convert action.", cls=CommandWithRollback)
@argument_query(True, "uuid", ["uuid", "checksum", "puid", "relative_path", "action", "warning", "processed", "lock"])
@argument("reason", nargs=1, type=str, required=True)
@option("--tool", type=str, required=True, help="The tool to use for conversion.")
@option(
    "--output",
    type=str,
    default=None,
    callback=param_callback_regex("^[a-zA-Z0-9-]+$"),
    help='The output of the converter.  [required for tools other than "copy"]',
)
@option("--lock", is_flag=True, default=False, help="Lock the edited files.")
@option_dry_run()
@pass_context
def cmd_action_original_convert(
    ctx: Context,
    query: TQuery,
    reason: str,
    tool: str,
    output: str | None,
    lock: bool,
    dry_run: bool,
):
    """
    Set the action of original files matching the QUERY argument to "convert".

    The --output option may be omitted when using the "copy" tool.

    To lock the file(s) after editing them, use the --lock option.

    To see the changes without committing them, use the --dry-run option.

    For details on the QUERY argument, see the edit command.
    """
    if tool not in ("copy",) and not output:
        raise MissingParameter(f"Required for tool {tool!r}.", ctx, ctx_params(ctx)["output"])

    data = ConvertAction(tool=tool, output=output)

    avid = get_avid(ctx)

    with open_database(ctx, avid) as database:
        log_file, log_stdout, _ = start_program(ctx, database, __version__, None, True, True, dry_run)

        with ExceptionManager(BaseException) as exception:
            for file in query_table(database.original_files, query, [("lower(relative_path)", "asc")]):
                set_action(ctx, database, file, "convert", data, reason, dry_run, log_stdout)
                if lock:
                    set_lock(ctx, database, file, reason, dry_run, log_stdout)

        end_program(ctx, database, exception, dry_run, log_file, log_stdout)


@rollback("edit", rollback_set_action)
@grp_action_original.command("extract", no_args_is_help=True, short_help="Set extract action.", cls=CommandWithRollback)
@argument_query(True, "uuid", ["uuid", "checksum", "puid", "relative_path", "action", "warning", "processed", "lock"])
@argument("reason", nargs=1, type=str, required=True)
@option("--tool", type=str, required=True, help="The tool to use for extraction.")
@option(
    "--extension",
    type=str,
    callback=param_callback_regex(r"^(.[a-zA-Z0-9]+)+$"),
    help="The extension the file must have for extraction to succeed.",
)
@option("--lock", is_flag=True, default=False, help="Lock the edited files.")
@option_dry_run()
@pass_context
def cmd_action_original_extract(
    ctx: Context,
    query: TQuery,
    reason: str,
    tool: str,
    extension: str | None,
    lock: bool,
    dry_run: bool,
):
    """
    Set the action of original files matching the QUERY argument to "extract".

    To lock the file(s) after editing them, use the --lock option.

    To see the changes without committing them, use the --dry-run option.

    For details on the QUERY argument, see the edit command.
    """
    data = ExtractAction(tool=tool, extension=extension)

    avid = get_avid(ctx)

    with open_database(ctx, avid) as database:
        log_file, log_stdout, _ = start_program(ctx, database, __version__, None, True, True, dry_run)

        with ExceptionManager(BaseException) as exception:
            for file in query_table(database.original_files, query, [("lower(relative_path)", "asc")]):
                set_action(ctx, database, file, "extract", data, reason, dry_run, log_stdout)
                if lock:
                    set_lock(ctx, database, file, reason, dry_run, log_stdout)

        end_program(ctx, database, exception, dry_run, log_file, log_stdout)


@rollback("edit", rollback_set_action)
@grp_action_original.command("manual", no_args_is_help=True, short_help="Set manual action.", cls=CommandWithRollback)
@argument_query(True, "uuid", ["uuid", "checksum", "puid", "relative_path", "action", "warning", "processed", "lock"])
@argument("reason", nargs=1, type=str, required=True)
@option(
    "--reason",
    "data_reason",
    type=str,
    required=True,
    callback=param_callback_regex(r"^.*\S.*$"),
    help="The reason why the file must be processed manually.",
)
@option(
    "--process",
    type=str,
    required=True,
    callback=param_callback_regex(r"^.*\S.*$"),
    help="The steps to take to process the file.",
)
@option("--lock", is_flag=True, default=False, help="Lock the edited files.")
@option_dry_run()
@pass_context
def cmd_action_original_manual(
    ctx: Context,
    query: TQuery,
    reason: str,
    data_reason: str | None,
    process: str,
    lock: bool,
    dry_run: bool,
):
    """
    Set the action of original files matching the QUERY argument to "manual".

    To lock the file(s) after editing them, use the --lock option.

    To see the changes without committing them, use the --dry-run option.

    For details on the QUERY argument, see the edit command.
    """
    data = ManualAction(reason=data_reason, process=process)

    avid = get_avid(ctx)

    with open_database(ctx, avid) as database:
        log_file, log_stdout, _ = start_program(ctx, database, __version__, None, True, True, dry_run)

        with ExceptionManager(BaseException) as exception:
            for file in query_table(database.original_files, query, [("lower(relative_path)", "asc")]):
                set_action(ctx, database, file, "manual", data, reason, dry_run, log_stdout)
                if lock:
                    set_lock(ctx, database, file, reason, dry_run, log_stdout)

        end_program(ctx, database, exception, dry_run, log_file, log_stdout)


@rollback("edit", rollback_set_action)
@grp_action_original.command("ignore", no_args_is_help=True, short_help="Set ignore action.", cls=CommandWithRollback)
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
    callback=param_callback_regex(r"^.*\S.*$"),
    help='The reason why the file is ignored.  [required for "text" template]',
)
@option("--lock", is_flag=True, default=False, help="Lock the edited files.")
@option_dry_run()
@pass_context
@docstring_format(templates="\n".join(f"* {t}" for t in TemplateTypeEnum))
def cmd_action_original_ignore(
    ctx: Context,
    query: TQuery,
    reason: str,
    template: TTemplateType,
    data_reason: str | None,
    lock: bool,
    dry_run: bool,
):
    """
    Set the action of original files matching the QUERY argument to "ignore".

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

    avid = get_avid(ctx)

    with open_database(ctx, avid) as database:
        log_file, log_stdout, _ = start_program(ctx, database, __version__, None, True, True, dry_run)

        with ExceptionManager(BaseException) as exception:
            for file in query_table(database.original_files, query, [("lower(relative_path)", "asc")]):
                set_action(ctx, database, file, "ignore", data, reason, dry_run, log_stdout)
                if lock:
                    set_lock(ctx, database, file, reason, dry_run, log_stdout)

        end_program(ctx, database, exception, dry_run, log_file, log_stdout)


@rollback("edit", rollback_set_action)
@grp_action_original.command(
    "copy",
    no_args_is_help=True,
    short_help="Copy action from a format.",
    cls=CommandWithRollback,
)
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
    help="Path to a YAML file containing file format actions.",
)
@option("--lock", is_flag=True, default=False, help="Lock the edited files.")
@option_dry_run()
@pass_context
def cmd_action_original_copy(
    ctx: Context,
    puid: str,
    action: TActionType,
    reason: str,
    query: TQuery,
    actions_file: str | None,
    lock: bool,
    dry_run: bool,
):
    """
    Set the action of original files matching the QUERY argument by copying it from an existing format.

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
    avid = get_avid(ctx)

    with open_database(ctx, avid) as database:
        actions = fetch_actions(ctx, "actions_file", actions_file)

        if not (action_model := actions.get(puid)):
            raise BadParameter(f"Format {puid} not found.", ctx, ctx_params(ctx)["puid"])

        action_data: dict[str, Any] = action_model.action_data.model_dump()

        if not (data := action_data.get(action)):
            raise BadParameter(f"Action {action} not found in {puid}.", ctx, ctx_params(ctx)["puid"])

        log_file, log_stdout, _ = start_program(ctx, database, __version__, None, True, True, dry_run)

        with ExceptionManager(BaseException) as exception:
            for file in query_table(database.original_files, query, [("lower(relative_path)", "asc")]):
                set_action(ctx, database, file, action, data, reason, dry_run, log_stdout)
                if lock:
                    set_lock(ctx, database, file, reason, dry_run, log_stdout)

        end_program(ctx, database, exception, dry_run, log_file, log_stdout)


@rollback("edit", rollback_set_master_convert)
@command("convert", no_args_is_help=True, short_help="Set access convert action.", cls=CommandWithRollback)
@argument("action_type", type=Choice(["access", "statutory"]), required=True)
@argument_query(True, "uuid", ["uuid", "checksum", "puid", "relative_path", "action", "warning", "processed"])
@argument("reason", nargs=1, type=str, required=True)
@option("--tool", type=str, required=True, help="The tool to use for conversion.")
@option(
    "--output",
    type=str,
    default=None,
    callback=param_callback_regex("^[a-zA-Z0-9-]+$"),
    help='The output of the converter.  [required for tools other than "copy"]',
)
@option("--lock", is_flag=True, default=False, help="Lock the edited files.")
@option_dry_run()
@pass_context
def cmd_action_master_convert(
    ctx: Context,
    action_type: Literal["access", "statutory"],
    query: TQuery,
    reason: str,
    tool: str,
    output: str | None,
    dry_run: bool,
):
    """
    Set the convert actions of master files matching the QUERY argument.

    The --output option may be omitted when using the "copy" tool.

    To lock the file(s) after editing them, use the --lock option.

    To see the changes without committing them, use the --dry-run option.

    For details on the QUERY argument, see the edit command.
    """
    if tool not in ("copy",) and not output:
        raise MissingParameter(f"Required for tool {tool!r}.", ctx, ctx_params(ctx)["output"])

    data = ConvertAction(tool=tool, output=output)

    avid = get_avid(ctx)

    with open_database(ctx, avid) as database:
        log_file, log_stdout, _ = start_program(ctx, database, __version__, None, True, True, dry_run)

        with ExceptionManager(BaseException) as exception:
            for file in query_table(database.master_files, query, [("lower(relative_path)", "asc")]):
                set_master_convert(ctx, database, file, data, action_type, reason, dry_run)

        end_program(ctx, database, exception, dry_run, log_file, log_stdout)


grp_action_original.list_commands = lambda _ctx: list(grp_action_original.commands)
