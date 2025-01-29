from datetime import datetime
from logging import ERROR
from logging import INFO
from logging import Logger
from logging import WARN
from re import fullmatch
from sqlite3 import DatabaseError
from typing import Literal
from uuid import UUID

from acacore.database import FilesDB
from acacore.models.event import Event
from acacore.models.file import BaseFile
from acacore.utils.click import end_program
from acacore.utils.click import start_program
from acacore.utils.helpers import ExceptionManager
from click import argument
from click import BadParameter
from click import command
from click import Context
from click import DateTime
from click import option
from click import Parameter
from click import pass_context

from digiarch.__version__ import __version__
from digiarch.common import _RH
from digiarch.common import AVID
from digiarch.common import find_rollback_handlers
from digiarch.common import get_avid
from digiarch.common import open_database
from digiarch.common import option_dry_run


def callback_arg_run(ctx: Context, param: Parameter, value: str) -> tuple[int, int | None] | datetime:
    if fullmatch(r"\d+(:\d+)?", value):
        start, _, end = value.partition(":")
        start_index, end_index = int(start), int(end or start)
        if start_index < 1 or end_index < 1:
            raise BadParameter("index must be a positive non-zero integer.")
        if start_index > end_index:
            start_index, end_index = end_index, start_index
        if start_index == end_index:
            end_index = None
        return start_index, end_index

    return DateTime(["%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S"]).convert(value, param, ctx)


def fetch_runs(database: FilesDB, run: tuple[int, int | None] | datetime, exclude_op: str) -> list[tuple[Event, Event]]:
    runs_start: list[Event]
    runs: list[tuple[Event, Event]]

    if isinstance(run, tuple):
        runs_start = database.log.select(
            "operation like '%:start' and operation != ?",
            [exclude_op],
            order_by=[("time", "desc")],
            offset=run[0] - 1,
            limit=(run[1] - run[0] + 1) if run[1] else 1,
        ).fetchall()
    else:
        runs_start = database.log.select(
            "time <= ? and operation like '%:start' and operation != ?",
            [run.isoformat(), exclude_op],
            limit=1,
        ).fetchall()

    runs = [
        (r, e)
        for r in runs_start
        if (
            e := database.log.select(
                "time >= ? and operation = ?",
                [r.time.isoformat(), r.operation.split(":")[0] + ":end"],
                order_by=[("time", "desc")],
                limit=1,
            ).fetchone()
        )
    ]

    return runs


def get_file(
    database: FilesDB,
    file_type: Literal["original", "master", "access", "statutory"] | None,
    file_uuid: UUID | None,
) -> BaseFile | None:
    if not file_type or not file_uuid:
        return None
    if file_type == "original":
        return database.original_files.select("uuid = ?", [str(file_uuid)], limit=1).fetchone()
    if file_type == "master":
        return database.master_files.select("uuid = ?", [str(file_uuid)], limit=1).fetchone()
    if file_type == "access":
        return database.access_files.select("uuid = ?", [str(file_uuid)], limit=1).fetchone()
    if file_type == "statutory":
        return database.statutory_files.select("uuid = ?", [str(file_uuid)], limit=1).fetchone()
    return None


def rollback(
    ctx: Context,
    avid: AVID,
    database: FilesDB,
    handlers: dict[str, _RH],
    runs: list[tuple[Event, Event]],
    rolled_runs: set[datetime],
    dry_run: bool,
    *loggers: Logger,
):
    runs.sort(key=lambda r: -r[0].time.timestamp())

    for run_start, run_end in runs:
        if run_start.time in rolled_runs:
            Event.from_command(ctx, "skip").log(
                INFO,
                *loggers,
                run=f"{run_start.time:%Y-%m-%dT%T}",
                reason="Already rolled back",
            )
            continue

        partial: bool = False

        try:
            for event in database.log.select(
                "time >= ? and time <= ? and operation not in (?, ?)",
                [run_start.time.isoformat(), run_end.time.isoformat(), run_start.operation, run_end.operation],
            ):
                if not event.file_uuid:
                    continue
                if not event.file_type:
                    continue
                if not (handler := handlers.get(event.operation)):
                    continue

                file = get_file(database, event.file_type, event.file_uuid)

                with ExceptionManager(BaseException, allow=[KeyboardInterrupt, DatabaseError]) as exception:
                    if not dry_run:
                        handler(ctx, avid, database, event, file)
                        database.commit()
                    partial = True

                Event.from_command(
                    ctx,
                    "error" if exception.exception else "event",
                    (event.file_uuid, event.file_type),
                ).log(
                    ERROR if exception.exception else INFO,
                    *loggers,
                    run=f"{run_start.time:%Y-%m-%dT%T}",
                    event=f"{event.time:%Y-%m-%dT%T} {event.operation}",
                    **({"reason": repr(exception.exception)} if exception.exception else {}),
                )

            if not dry_run:
                database.log.insert(Event.from_command(ctx, "run", None, run_start))
        except BaseException as err:
            if partial:
                event = Event.from_command(ctx, "run:partial", None, run_start, repr(err))
                if not dry_run:
                    database.log.insert(event)
                event.log(WARN, *loggers, show_args=False, run=f"{run_start.time:%Y-%m-%dT%T}", reason=repr(err))
            raise


def opt_list_commands(ctx: Context, _param: Parameter, value: bool):
    if value:
        handlers = [" ".join(h.split(":")[0].split(".")) for h in find_rollback_handlers(ctx.find_root().command)]
        print(*sorted(set(handlers), key=handlers.index), sep="\n")
        ctx.exit(0)


@command("rollback", no_args_is_help=True, short_help="Roll back edits.")
@argument("run", nargs=1, type=str, required=True, callback=callback_arg_run)
@option("--resume-partial", is_flag=True, default=False, help="Ignore partially rolled back runs.")
@option(
    "--list-commands",
    is_flag=True,
    is_eager=True,
    help="List commands that can be rolled back.",
    callback=opt_list_commands,
    expose_value=False,
)
@option_dry_run()
@pass_context
def cmd_rollback(ctx: Context, run: tuple[int, int | None] | datetime, resume_partial: bool, dry_run: bool):
    """
    Roll back changes.

    RUN can be a run index (1 for the previous run, 2 for the run before that, and so on), an index slice
    (e.g., 2:4 to roll back the second to last through the fourth to last run) or the timestamp of a run in the format
    '%Y-%m-%dT%H:%M:%S' or '%Y-%m-%dT%H:%M:%S.%f'.

    Runs that have already been rolled back (even if just partially) are ignored. To include partially rolled-back runs
    use the --resume-partial option.

    To see the changes without committing them, use the --dry-run option.

    To see a list of commands that can be rolled back, use the --list-commands option.
    """
    handlers = find_rollback_handlers(ctx.find_root().command)

    avid = get_avid(ctx)

    with open_database(ctx, avid) as database:
        _, log_stdout, start_event = start_program(ctx, database, __version__, None, False, True, dry_run)

        with ExceptionManager(BaseException) as exception:
            runs = fetch_runs(database, run, start_event.operation)
            rolled_runs: set[tuple[datetime, bool]] = {
                (Event.model_validate(r.data).time, r.operation.endswith(":run:partial"))
                for r in database.log.select(
                    "operation in (?, ?)",
                    [
                        f"{start_event.operation.split(':')[0]}:run",
                        f"{start_event.operation.split(':')[0]}:run:partial",
                    ],
                ).fetchall()
            }

            if runs:
                rollback(
                    ctx,
                    avid,
                    database,
                    handlers,
                    runs,
                    {t for t, p in rolled_runs if not resume_partial or not p},
                    dry_run,
                    log_stdout,
                )
            else:
                Event.from_command(ctx, "skip", reason="No matching runs found").log(INFO, log_stdout)

        end_program(ctx, database, exception, dry_run, log_stdout)
