from sys import stdout

import yaml
from acacore.database.table import Table
from acacore.models.file import BaseFile
from acacore.utils.decorators import docstring_format
from click import Choice
from click import Context
from click import group
from click import IntRange
from click import option
from click import pass_context

from digiarch.common import get_avid
from digiarch.common import open_database
from digiarch.query import argument_query
from digiarch.query import query_table
from digiarch.query import TQuery


def search_table(table: Table[BaseFile], query: TQuery, sort: str, order: str, limit: int, offset: int):
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

    for file in query_table(table, query, [(sort, order)], limit, offset):
        model_dump = file.model_dump(mode="json")
        del model_dump["root"]
        yaml.dump(model_dump, stdout, yaml.Dumper, sort_keys=False)
        print()


@group("search", no_args_is_help=True, short_help="Search the database.")
def grp_search():
    """Search files in the database."""


@grp_search.command("original", no_args_is_help=True, short_help="Search original files.")
@argument_query(
    False,
    "uuid",
    [
        "uuid",
        "checksum",
        "puid",
        "relative_path",
        "action",
        "warning",
        "is_binary",
        "processed",
        "lock",
        "original_path",
    ],
)
@option(
    "--sort",
    type=Choice(["relative_path", "puid", "checksum", "action"]),
    default="relative_path",
    show_default=True,
    help="Choose sorting column,",
)
@option("--order", type=Choice(["asc", "desc"]), default="asc", show_default=True, help="Choose sorting order.")
@option("--limit", metavar="INTEGER", type=IntRange(1), default=100, show_default=True, help="Limit number of results.")
@option("--offset", metavar="INTEGER", type=IntRange(0), default=0, show_default=True, help="Offset number of results.")
@pass_context
@docstring_format(
    fields="\n".join(
        f"* {f}"
        for f in [
            "uuid",
            "checksum",
            "puid",
            "relative_path",
            "action",
            "warning",
            "is_binary",
            "processed",
            "lock",
            "original_path",
        ]
    )
)
def cmd_search_original(ctx: Context, query: TQuery, sort: str, order: str, limit: int, offset: int):
    """
    Search among the original files in the database.

    \b
    The wollowing query fields are supported:
    {fields}

    For details on the QUERY argument, see the edit command.
    """  # noqa: D301
    with open_database(ctx, get_avid(ctx)) as database:
        search_table(database.original_files, query, sort, order, limit, offset)


@grp_search.command("master", no_args_is_help=True, short_help="Search master files.")
@argument_query(
    False,
    "uuid",
    ["uuid", "checksum", "puid", "relative_path", "warning", "is_binary", "processed", "original_uuid"],
)
@option(
    "--sort",
    type=Choice(["relative_path", "puid", "checksum"]),
    default="relative_path",
    show_default=True,
    help="Choose sorting column,",
)
@option("--order", type=Choice(["asc", "desc"]), default="asc", show_default=True, help="Choose sorting order.")
@option("--limit", metavar="INTEGER", type=IntRange(1), default=100, show_default=True, help="Limit number of results.")
@option("--offset", metavar="INTEGER", type=IntRange(0), default=0, show_default=True, help="Offset number of results.")
@pass_context
@docstring_format(
    fields="\n".join(
        f"* {f}"
        for f in ["uuid", "checksum", "puid", "relative_path", "warning", "is_binary", "processed", "original_uuid"]
    )
)
def cmd_search_master(ctx: Context, query: TQuery, sort: str, order: str, limit: int, offset: int):
    """
    Search among the master files in the database.

    \b
    The wollowing query fields are supported:
    {fields}

    For details on the QUERY argument, see the edit command.
    """  # noqa: D301
    with open_database(ctx, get_avid(ctx)) as database:
        search_table(database.master_files, query, sort, order, limit, offset)


@grp_search.command("access", no_args_is_help=True, short_help="Search access files.")
@argument_query(False, "uuid", ["uuid", "checksum", "puid", "relative_path", "warning", "is_binary", "original_uuid"])
@option(
    "--sort",
    type=Choice(["relative_path", "puid", "checksum"]),
    default="relative_path",
    show_default=True,
    help="Choose sorting column,",
)
@option("--order", type=Choice(["asc", "desc"]), default="asc", show_default=True, help="Choose sorting order.")
@option("--limit", metavar="INTEGER", type=IntRange(1), default=100, show_default=True, help="Limit number of results.")
@option("--offset", metavar="INTEGER", type=IntRange(0), default=0, show_default=True, help="Offset number of results.")
@pass_context
@docstring_format(
    fields="\n".join(
        f"* {f}" for f in ["uuid", "checksum", "puid", "relative_path", "warning", "is_binary", "original_uuid"]
    )
)
def cmd_search_access(ctx: Context, query: TQuery, sort: str, order: str, limit: int, offset: int):
    """
    Search among the access files in the database.

    \b
    The wollowing query fields are supported:
    {fields}

    For details on the QUERY argument, see the edit command.
    """  # noqa: D301
    with open_database(ctx, get_avid(ctx)) as database:
        search_table(database.access_files, query, sort, order, limit, offset)


@grp_search.command("statutory", no_args_is_help=True, short_help="Search statutory files.")
@argument_query(False, "uuid", ["uuid", "checksum", "puid", "relative_path", "warning", "is_binary", "original_uuid"])
@option(
    "--sort",
    type=Choice(["relative_path", "puid", "checksum"]),
    default="relative_path",
    show_default=True,
    help="Choose sorting column,",
)
@option("--order", type=Choice(["asc", "desc"]), default="asc", show_default=True, help="Choose sorting order.")
@option("--limit", metavar="INTEGER", type=IntRange(1), default=100, show_default=True, help="Limit number of results.")
@option("--offset", metavar="INTEGER", type=IntRange(0), default=0, show_default=True, help="Offset number of results.")
@pass_context
@docstring_format(
    fields="\n".join(
        f"* {f}" for f in ["uuid", "checksum", "puid", "relative_path", "warning", "is_binary", "original_uuid"]
    )
)
def cmd_search_statutory(ctx: Context, query: TQuery, sort: str, order: str, limit: int, offset: int):
    """
    Search among the statutory files in the database.

    \b
    The wollowing query fields are supported:
    {fields}

    For details on the QUERY argument, see the edit command.
    """  # noqa: D301
    with open_database(ctx, get_avid(ctx)) as database:
        search_table(database.statutory_files, query, sort, order, limit, offset)
