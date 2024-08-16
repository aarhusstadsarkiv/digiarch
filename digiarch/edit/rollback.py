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


def rollback_edit_action(database: FileDB, event: HistoryEntry, dry_run: bool) -> tuple[File | None, str | None]:
    file = database.files.select(where="uuid = ?", parameters=[str(event.uuid)]).fetchone()
    if file and not dry_run:
        # noinspection PyUnresolvedReferences
        old_action: TActionType | None = event.data[0]
        # noinspection PyUnresolvedReferences
        old_action_data: dict[TActionType, dict | None] = event.data[2]
        file.action = old_action
        file.action_data = ActionData.model_validate(file.action_data.model_dump() | old_action_data)
        database.files.update(file, {"uuid": file.uuid})
    return file, None


def rollback_edit_lock(database: FileDB, event: HistoryEntry, dry_run: bool) -> tuple[File | None, str | None]:
    file = database.files.select(where="uuid = ?", parameters=[str(event.uuid)]).fetchone()
    if file and not dry_run:
        # noinspection PyUnresolvedReferences
        file.lock = event.data[0]
        database.files.update(file, {"uuid": file.uuid})
    return file, None


def rollback_edit_rename(
    database: FileDB, root: Path, event: HistoryEntry, dry_run: bool
) -> tuple[File | None, str | None]:
    file = database.files.select(where="uuid = ?", parameters=[str(event.uuid)]).fetchone()
    if file and not dry_run:
        file.root = root
        # noinspection PyUnresolvedReferences
        old_name: str = event.data[0]
        if (old_path := file.get_absolute_path().with_name(old_name)).is_file():
            return None, "file already exists"
        file.get_absolute_path().rename(old_path)
        file.name = old_name
        database.files.update(file, {"uuid": file.uuid})
    return file, None


def rollback_edit_remove(
    database: FileDB, root: Path, event: HistoryEntry, dry_run: bool
) -> tuple[File | None, str | None]:
    file = File.model_validate(event.data)
    if not file.get_absolute_path(root).is_file():
        return None, "file does not exist"
    elif file and not dry_run:
        database.files.insert(file)
    return file, None


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
@option(
    "--command",
    "commands",
    type=str,
    multiple=True,
    callback=param_regex(r"^[a-z-]+(.[a-z-]+)*$"),
    help="Specify commands to roll back.  [multiple]",
)
@option_dry_run()
@pass_context
def command_rollback(
    ctx: Context,
    root: Path,
    reason: str,
    time_from: datetime,
    time_to: datetime,
    commands: tuple[str, ...],
    dry_run: bool,
):
    """
    Roll back edits between two timestamps.

    FROM and TO timestamps must be in the format '%Y-%m-%dT%H:%M:%S' or '%Y-%m-%dT%H:%M:%S.%f'.

    Using the --command option allows to restrict rollbacks to specific events with the given commands if the
    timestamps are not precise enough. E.g., "digiarch.edit.rename" to roll back changes performed by the "edit rename"
    command.

    To see the changes without committing them, use the --dry-run option.
    """
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
            if commands := tuple(c.strip() for c in commands if c.strip()):
                where += " and operation like ? || ':%'" * len(commands)
                parameters.extend(commands)
            events: list[HistoryEntry] = list(
                database.history.select(where=where, parameters=parameters, order_by=[("time", "desc")])
            )
            for event in events:
                name, _, operation = event.operation.partition(":")
                file: File | None

                if name.startswith(f"{program_name}.{group_edit.name}.{group_action.name}"):
                    if operation != "edit":
                        continue
                    file, error = rollback_edit_action(database, event, dry_run)
                elif name == f"{program_name}.{group_edit.name}.{command_lock.name}":
                    if operation != "edit":
                        continue
                    file, error = rollback_edit_lock(database, event, dry_run)
                elif name == f"{program_name}.{group_edit.name}.{command_rename.name}":
                    if operation != "edit":
                        continue
                    file, error = rollback_edit_rename(database, root, event, dry_run)
                elif name == f"{program_name}.{group_edit.name}.{command_remove.name}":
                    if operation != "remove":
                        continue
                    file, error = rollback_edit_remove(database, root, event, dry_run)
                else:
                    HistoryEntry.command_history(ctx, "warning", None, event.operation, "Unknown event").log(
                        WARNING, log_stdout
                    )
                    continue

                if not file:
                    error_reason = "File cannot be restored"
                    error_reason += f": {error}" if error else ""
                    HistoryEntry.command_history(
                        ctx, "error", event.uuid, [event.time.isoformat(), event.operation], error_reason
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
