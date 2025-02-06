from functools import reduce
from sys import stdout

import yaml
from acacore.models.event import Event
from click import Choice
from click import command
from click import Context
from click import IntRange
from click import option
from click import pass_context

from digiarch.common import get_avid
from digiarch.common import open_database
from digiarch.query import query_table
from digiarch.query import TQuery


@command("log", short_help="Display the event log.")
@option("--runs-only", is_flag=True, default=False, help="Only show start/end events.")
@option("--order", type=Choice(["asc", "desc"]), default="asc", show_default=True, help="Choose sorting order.")
@option("--limit", metavar="INTEGER", type=IntRange(1), default=100, show_default=True, help="Limit number of results.")
@pass_context
def cmd_log(ctx: Context, runs_only: bool, order: str, limit: int):
    """
    Display the event log.

    Start events will display the index to be used to roll back the of that command.
    """
    yaml.add_representer(
        str,
        lambda dumper, data: (
            dumper.represent_str(str(data))
            if len(data) < 200
            else dumper.represent_scalar("tag:yaml.org,2002:str", str(data), style="|")
        ),
    )

    query: TQuery = []

    if runs_only:
        query = [("operation", "%:start", "like"), ("operation", "%:end", "like")]

    with open_database(ctx, get_avid(ctx)) as database:
        events: list[Event] = list(query_table(database.log, query, [("time", order)], limit))

    runs: int = reduce(
        lambda acc, cur: acc + int("rollback" not in cur.operation and cur.operation.endswith(":start")),
        events,
        0,
    )
    run_index: int = 1 if order == "desc" else runs

    for n, event in enumerate(events):
        model_dump = {k: v for k, v in event.model_dump(mode="json").items() if v is not None}
        if "rollback" not in event.operation and event.operation.endswith(":start"):
            model_dump = {"rollback-index": run_index} | model_dump
            run_index += 1 if order == "desc" else -1
        yaml.dump(model_dump, stdout, yaml.Dumper, sort_keys=False)
        print()
