from logging import INFO
from logging import Logger
from typing import Any
from typing import Literal

from acacore.database import FilesDB
from acacore.database.table import Table
from acacore.models.event import Event
from acacore.models.file import BaseFile
from click import Context

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
    property_value: Any,  # noqa: ANN401
    dry_run: bool,
    *loggers: Logger,
):
    for file in query_table(table, query, [("lower(relative_path)", "asc")]):
        if getattr(file, property_name) == property_value:
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
            [getattr(file, property_name), property_value],
            reason,
        )
        if not dry_run:
            setattr(file, property_name, property_value)
            table.update(file)
            database.log.insert(event)
        event.log(INFO, *loggers, show_args=["uuid", "data"], path=file.relative_path)
