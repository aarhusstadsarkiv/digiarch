from collections.abc import Callable
from logging import INFO
from logging import Logger
from typing import Any
from typing import Literal

from acacore.database import FilesDB
from acacore.database.table import Table
from acacore.models.event import Event
from acacore.models.file import BaseFile
from click import Context

from digiarch.common import _RH
from digiarch.common import AVID
from digiarch.query import query_table
from digiarch.query import TQuery


def edit_file_value(
    ctx: Context,
    database: FilesDB,
    table: Table[BaseFile],
    query: TQuery,
    reason: str,
    file_type: Literal["original", "master", "access", "statutory"],
    property_name: str,
    property_value: Callable[[Any], Any] | Any,  # noqa: ANN401
    dry_run: bool,
    *loggers: Logger,
    lock: bool = False,
):
    for file in query_table(table, query, [("lower(relative_path)", "asc")]):
        value = property_value(getattr(file, property_name)) if callable(property_value) else property_value
        if getattr(file, property_name) == value:
            Event.from_command(ctx, "skip", (file.uuid, file_type), reason="No Changes").log(
                INFO,
                *loggers,
                path=file.relative_path,
            )
            continue
        event = Event.from_command(
            ctx,
            "edit",
            (file.uuid, file_type),
            [getattr(file, property_name), value],
            reason,
        )
        if not dry_run:
            setattr(file, property_name, value)
            if lock and hasattr(file, "lock"):
                setattr(file, "lock", True)
            table.update(file)
            database.log.insert(event)
            database.commit()
        event.log(INFO, *loggers, show_args=["uuid", "data"], path=file.relative_path)


def rollback_file_value(property_name: str) -> _RH:
    def _handler(_ctx: Context, _avid: AVID, database: FilesDB, event: Event, file: BaseFile | None):
        if not file:
            return
        if not event.file_type:
            return
        if event.file_type == "original":
            table = database.original_files
        elif event.file_type == "master":
            table = database.master_files
        elif event.file_type == "access":
            table = database.access_files
        elif event.file_type == "statutory":
            table = database.statutory_files
        else:
            return
        if not isinstance(file, table.model):
            raise TypeError(f"{type(file)} is not {table.model.__name__}")
        prev_value, next_value = event.data
        setattr(file, property_name, prev_value)
        table.update(file)
        database.commit()

    return _handler
