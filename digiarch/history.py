from datetime import datetime
from pathlib import Path
from re import IGNORECASE
from sys import stdout
from uuid import UUID

import yaml
from acacore.database import FileDB
from click import command
from click import Context
from click import DateTime
from click import option
from click import pass_context

from digiarch.common import argument_root
from digiarch.common import check_database_version
from digiarch.common import ctx_params
from digiarch.common import param_regex


@command("history", no_args_is_help=True, short_help="View and search events log.")
@argument_root(True)
@option(
    "--from",
    "time_from",
    type=DateTime(["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f"]),
    default=None,
    help="Minimum date of events.",
)
@option(
    "--to",
    "time_to",
    type=DateTime(["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f"]),
    default=None,
    help="Maximum date of events.",
)
@option(
    "--operation",
    type=str,
    default=None,
    multiple=True,
    callback=param_regex(r"[a-z%-]+(\.[a-z%-]+)*(:[a-z%-]+([.:][a-z%-]+)*)?", IGNORECASE),
    help="Operation and sub-operation.",
)
@option(
    "--uuid",
    type=str,
    default=None,
    multiple=True,
    callback=param_regex(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", IGNORECASE),
    help="File UUID.",
)
@option("--reason", type=str, default=None, multiple=True, help="Event reason.")
@option(
    "--ascending/--descending",
    "ascending",
    is_flag=True,
    default=True,
    show_default=True,
    help="Sort by ascending or descending order.",
)
@pass_context
def command_history(
    ctx: Context,
    root: Path,
    time_from: datetime | None,
    time_to: datetime | None,
    operation: tuple[str, ...] | None,
    uuid: tuple[str, ...] | None,
    reason: tuple[str, ...] | None,
    ascending: bool,
):
    """
    View and search events log.

    The --operation and --reason options supports LIKE syntax with the % operator.

    If multiple --uuid, --operation, or --reason options are used, the query will match any of them.

    If no query option is given, only the first 100 results will be shown.
    """
    check_database_version(ctx, ctx_params(ctx)["root"], (db_path := root / "_metadata" / "files.db"))

    operation = tuple(o.strip() for o in operation if o.strip(" %:.")) if operation else None
    reason = tuple(r.strip(" %") for r in reason if r.strip(" %")) if reason else None

    where: list[str] = []
    parameters: list[str | int] = []

    if time_from:
        where.append("time <= ?")
        parameters.append(time_from.isoformat())

    if time_to:
        where.append("time <= ?")
        parameters.append(time_to.isoformat())

    if uuid:
        where.append("(" + " or ".join("uuid = ?" for _ in uuid) + ")")
        parameters.extend(uuid)

    if operation:
        where.append("(" + " or ".join("operation like ?" for _ in operation) + ")")
        parameters.extend(operation)

    if reason:
        where.append("(" + " or ".join("reason like '%' || ? || '%'" for _ in reason) + ")")
        parameters.extend(reason)

    yaml.add_representer(UUID, lambda dumper, data: dumper.represent_str(str(data)))
    yaml.add_representer(
        str,
        lambda dumper, data: (
            dumper.represent_str(str(data))
            if len(data) < 200
            else dumper.represent_scalar("tag:yaml.org,2002:str", str(data), style="|")
        ),
    )

    with FileDB(db_path) as database:
        for event in database.history.select(
            where=" and ".join(where) or None,
            parameters=parameters or None,
            order_by=[("time", "asc" if ascending else "desc")],
            limit=None if where else 100,
        ):
            yaml.dump(event.model_dump(), stdout, yaml.Dumper, sort_keys=False)
            print()
