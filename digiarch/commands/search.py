from pathlib import Path
from sys import stdout

import yaml
from acacore.database import FileDB
from click import Choice
from click import command
from click import Context
from click import IntRange
from click import option
from click import pass_context

from digiarch.commands.edit.common import argument_query
from digiarch.commands.edit.common import find_files
from digiarch.commands.edit.common import TQuery
from digiarch.common import argument_root
from digiarch.common import check_database_version
from digiarch.common import ctx_params


@command("search", no_args_is_help=True, short_help="Search the database.")
@argument_root(True)
@argument_query(False, "uuid", ["uuid", "checksum", "puid", "relative_path", "action", "warning", "processed", "lock"])
@option(
    "--order-by",
    type=Choice(["relative_path", "size", "action"]),
    default="relative_path",
    show_default=True,
    show_choices=True,
    help="Set sorting field.",
)
@option(
    "--sort",
    type=Choice(["asc", "desc"]),
    default="asc",
    show_default=True,
    show_choices=True,
    help="Set sorting direction.",
)
@option("--limit", type=IntRange(1), default=None, help="Limit the number of results.")
@pass_context
def command_search(
    ctx: Context,
    root: Path,
    query: TQuery,
    order_by: str,
    sort: str,
    limit: int | None,
):
    """
    Search for specific files in the database.

    Files are displayed in YAML format.

    For details on the QUERY argument, see the edit command.

    If there is no query, then the limit is automatically set to 100 if not set with the --limit option.
    """
    check_database_version(ctx, ctx_params(ctx)["root"], (db_path := root / "_metadata" / "files.db"))

    yaml.add_representer(
        str,
        lambda dumper, data: (
            dumper.represent_str(str(data))
            if len(data) < 200
            else dumper.represent_scalar("tag:yaml.org,2002:str", str(data), style="|")
        ),
    )

    if not query:
        limit = limit or 100

    with FileDB(db_path) as database:
        for file in find_files(database, query, [(order_by, sort)], limit):
            model_dump = file.model_dump(mode="json")
            del model_dump["root"]
            yaml.dump(model_dump, stdout, yaml.Dumper, sort_keys=False)
            print()
