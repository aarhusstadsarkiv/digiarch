from datetime import datetime
from logging import ERROR
from logging import INFO
from logging import WARNING
from pathlib import Path

from acacore.database import FileDB
from acacore.models.file import File
from acacore.models.history import HistoryEntry
from acacore.models.reference_files import ActionData
from acacore.models.reference_files import TActionType
from acacore.utils.helpers import ExceptionManager
from click import argument
from click import command
from click import Context
from click import DateTime
from click import option
from click import pass_context

from digiarch.common import argument_root
from digiarch.common import check_database_version
from digiarch.common import ctx_params
from digiarch.common import end_program
from digiarch.common import option_dry_run
from digiarch.common import param_regex
from digiarch.common import start_program


def rollback_edit_action(database: FileDB, event: HistoryEntry, dry_run: bool) -> File | None:
    file = database.files.select(where="uuid = ?", parameters=[str(event.uuid)]).fetchone()
    if file and not dry_run:
        # noinspection PyUnresolvedReferences
        old_action: TActionType | None = event.data[0]
        # noinspection PyUnresolvedReferences
        old_action_data: dict[TActionType, dict | None] = event.data[2]
        file.action = old_action
        file.action_data = ActionData.model_validate(file.action_data.model_dump() | old_action_data)
        database.files.update(file, {"uuid": file.uuid})
    return file


def rollback_edit_lock(database: FileDB, event: HistoryEntry, dry_run: bool) -> File | None:
    file = database.files.select(where="uuid = ?", parameters=[str(event.uuid)]).fetchone()
    if file and not dry_run:
        # noinspection PyUnresolvedReferences
        file.lock = event.data[0]
        database.files.update(file, {"uuid": file.uuid})
    return file


def rollback_edit_rename(database: FileDB, event: HistoryEntry, dry_run: bool) -> File | None:
    file = database.files.select(where="uuid = ?", parameters=[str(event.uuid)]).fetchone()
    if file and not dry_run:
        file.root = database.path.parent.parent
        # noinspection PyUnresolvedReferences
        file.get_absolute_path().rename(file.get_absolute_path().with_name(event.data[0]))
    return file


def rollback_edit_remove(database: FileDB, event: HistoryEntry, dry_run: bool) -> File | None:
    file = File.model_validate(event.data)
    if not file.get_absolute_path(database.path.parent.parent).is_file():
        return None
    elif file and not dry_run:
        database.files.insert(file)
    return file


@command("rollback", no_args_is_help=True, short_help="Roll back edits.")
@argument_root(True)
@argument(
    "time_from",
    metavar="FROM",
    nargs=1,
    type=DateTime(["%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f"]),
    required=True,
)
@argument(
    "time_to",
    metavar="TO",
    nargs=1,
    type=DateTime(["%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f"]),
    required=True,
)
@argument("reason", nargs=1, type=str, required=True)
@option("--command", "command_name", type=str, default=None, callback=param_regex(r"^\w+(.\w+)*$"))
@option_dry_run()
@pass_context
def command_rollback(
    ctx: Context,
    root: Path,
    reason: str,
    time_from: datetime,
    time_to: datetime,
    command_name: str | None,
    dry_run: bool,
):
    from .edit import command_lock
    from .edit import command_remove
    from .edit import command_rename
    from .edit import group_action
    from .edit import group_edit

    program_name: str = ctx.find_root().command.name

    check_database_version(ctx, ctx_params(ctx)["root"], (db_path := root / "_metadata" / "files.db"))

    with FileDB(db_path) as database:
        log_file, log_stdout = start_program(ctx, database, None, True, True, dry_run)

        with ExceptionManager(BaseException) as exception:
            where = "uuid is not null and time >= ? and time <= ?"
            parameters = [time_from.isoformat(), time_to.isoformat()]
            if command_name:
                where += " and operation like ? || ':%'"
                parameters.append(command_name)
            events: list[HistoryEntry] = list(
                database.history.select(where=where, parameters=parameters, order_by=[("time", "desc")])
            )
            for event in events:
                name, _, operation = event.operation.partition(":")
                file: File | None

                if name.startswith(f"{program_name}.{group_edit.name}.{group_action.name}"):
                    if operation != "edit":
                        continue
                    file = rollback_edit_action(database, event, dry_run)
                elif name == f"{program_name}.{group_edit.name}.{command_lock.name}":
                    if operation != "edit":
                        continue
                    file = rollback_edit_lock(database, event, dry_run)
                elif name == f"{program_name}.{group_edit.name}.{command_rename.name}":
                    if operation != "edit":
                        continue
                    file = rollback_edit_rename(database, event, dry_run)
                elif name == f"{program_name}.{group_edit.name}.{command_remove.name}":
                    if operation != "remove":
                        continue
                    file = rollback_edit_remove(database, event, dry_run)
                else:
                    HistoryEntry.command_history(ctx, "warning", None, event.operation, "Unknown event").log(
                        WARNING, log_stdout
                    )
                    continue

                if not file:
                    HistoryEntry.command_history(
                        ctx, "error", event.uuid, [event.time.isoformat(), event.operation], "File cannot be restored"
                    ).log(ERROR, log_file, log_stdout)
                    continue

                rollback_event = HistoryEntry.command_history(
                    ctx, "rollback", event.uuid, event.model_dump(mode="json"), reason
                )

                if not dry_run:
                    database.history.insert(rollback_event)

                rollback_event.data = event.operation
                rollback_event.log(INFO, log_stdout)

        end_program(ctx, database, exception, dry_run, log_file, log_stdout)
