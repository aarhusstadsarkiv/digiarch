from typing import Any
from typing import Callable

from acacore.database import FilesDB
from acacore.database.table import Table
from acacore.models.file import BaseFile
from click import command
from click import Context
from click import pass_context

from digiarch.common import get_avid
from digiarch.common import open_database


def count_size(table: Table[BaseFile]) -> int:
    return table.database.execute(f"select sum(size) from {table.name}").fetchone()[0] or 0


def count_warnings(table: Table[BaseFile]) -> int:
    return table.database.execute(
        f"select count(*) from {table.name} where (warning is not null or puid is null) and size != 0"
    ).fetchone()[0]


def count_unique(table: Table[BaseFile]) -> int:
    return table.database.execute(f"select count(distinct checksum) from {table.name}").fetchone()[0]


def count_processed_original(database: FilesDB) -> int:
    return database.execute(f"select count(*) from {database.original_files.name} where processed").fetchone()[0]


def count_processed_master_access(database: FilesDB) -> int:
    return database.execute(f"select count(*) from {database.master_files.name} where processed & 1 > 0").fetchone()[0]


def count_processed_master_statutory(database: FilesDB) -> int:
    return database.execute(f"select count(*) from {database.master_files.name} where processed & 2 > 0").fetchone()[0]


def count_runs(database: FilesDB) -> int:
    return database.execute(f"select count(*) from {database.log.name} where operation like '%:start'").fetchone()[0]


def count_errors(database: FilesDB) -> int:
    return database.execute(f"select count(*) from {database.log.name} where operation like '%:error'").fetchone()[0]


def lazy_print(*msgs: str | Callable[[], Any]):
    for msg in msgs:
        if callable(msg):
            print(msg(), end="", flush=True)
        else:
            print(msg, end="", flush=True)
    print()


def si_size(size: int) -> str:
    if size >= 1e10:
        return f"{size / 1e9:0.1f}GB"
    if size >= 1e7:
        return f"{size / 1e6:0.1f}MB"
    if size >= 1e4:
        return f"{size / 1e3:0.1f}KB"
    return str(size)


@command("info", short_help="Database information.")
@pass_context
def cmd_info(ctx: Context):
    """Display information about the database."""
    avid = get_avid(ctx)

    with open_database(ctx, avid) as database:
        print("files:")
        lazy_print("  original: ", lambda: len(database.original_files))
        lazy_print("  master: ", lambda: len(database.master_files))
        lazy_print("  access: ", lambda: len(database.access_files))
        lazy_print("  statutory: ", lambda: len(database.statutory_files))

        print("\nunique:")
        lazy_print("  original: ", lambda: count_unique(database.original_files))
        lazy_print("  master: ", lambda: count_unique(database.master_files))
        lazy_print("  access: ", lambda: count_unique(database.access_files))
        lazy_print("  statutory: ", lambda: count_unique(database.statutory_files))

        print("\nwarnings:")
        lazy_print("  original: ", lambda: count_warnings(database.original_files))
        lazy_print("  master: ", lambda: count_warnings(database.master_files))
        lazy_print("  access: ", lambda: count_warnings(database.access_files))
        lazy_print("  statutory: ", lambda: count_warnings(database.statutory_files))

        print("\nprocessed:")
        lazy_print("  original: ", lambda: count_processed_original(database))
        print("  master:")
        lazy_print("    access: ", lambda: count_processed_master_access(database))
        lazy_print("    statutory: ", lambda: count_processed_master_statutory(database))

        print("\nsize:")
        lazy_print("  original: ", lambda: si_size(count_size(database.original_files)))
        lazy_print("  master: ", lambda: si_size(count_size(database.master_files)))
        lazy_print("  access: ", lambda: si_size(count_size(database.access_files)))
        lazy_print("  statutory: ", lambda: si_size(count_size(database.statutory_files)))

        print("\nevents:")
        lazy_print("  runs: ", lambda: count_runs(database))
        lazy_print("  errors: ", lambda: count_errors(database))
