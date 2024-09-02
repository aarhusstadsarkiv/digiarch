from pathlib import Path
from pathlib import PosixPath
from pathlib import WindowsPath
from sys import stdout
from uuid import UUID

import yaml
from acacore.database import FileDB
from click import Choice
from click import command
from click import Context
from click import IntRange
from click import option
from click import pass_context

from digiarch.edit.common import argument_ids
from digiarch.edit.common import find_files

from .common import argument_root
from .common import check_database_version
from .common import ctx_params


@command("search", no_args_is_help=True, short_help="Search the database.")
@argument_root(True)
@argument_ids(True)
@option(
    "--order-by",
    type=Choice(["relative_path", "size", "action"]),
    default="relative_path",
    show_default=True,
    show_choices=True,
    help="Set sorting field.",
)
@option("--sort", type=Choice(["asc", "desc"]), default="asc", help="Set sorting direction.")
@option("--limit", type=IntRange(1), default=None, help="Limit the number of results.")
@pass_context
def command_search(
    ctx: Context,
    root: Path,
    ids: tuple[str],
    id_type: str,
    id_files: bool,
    order_by: str,
    sort: str,
    limit: int | None,
):
    check_database_version(ctx, ctx_params(ctx)["root"], (db_path := root / "_metadata" / "files.db"))

    yaml.add_representer(
        str,
        lambda dumper, data: (
            dumper.represent_str(str(data))
            if len(data) < 200
            else dumper.represent_scalar("tag:yaml.org,2002:str", str(data), style="|")
        ),
    )

    with FileDB(db_path) as database:
        for file in find_files(database, ids, id_type, id_files, [(order_by, sort)], limit):
            model_dump = file.model_dump(mode="json")
            del model_dump["root"]
            yaml.dump(model_dump, stdout, yaml.Dumper, sort_keys=False)
            print()
