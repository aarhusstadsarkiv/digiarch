from pathlib import Path
from typing import Any
from typing import Callable
from typing import Generator
from typing import TypeVar

from acacore.database import FileDB
from acacore.models.file import File
from click import argument
from click import option

FC = TypeVar("FC", bound=Callable[..., Any])


def argument_ids(required: bool) -> Callable[[FC], FC]:
    def inner(callback: FC) -> FC:
        decorators: list[Callable[[FC], FC]] = [
            argument(
                "ids",
                metavar="ID...",
                nargs=-1,
                type=str,
                required=required,
                callback=lambda _c, _p, v: tuple(sorted(set(v), key=v.index)),
            ),
            option(
                "--uuid",
                "id_type",
                flag_value="uuid",
                default=True,
                help="Use UUIDs as identifiers.  [default]",
            ),
            option(
                "--puid",
                "id_type",
                flag_value="puid",
                help="Use PUIDs as identifiers.",
            ),
            option(
                "--path",
                "id_type",
                flag_value="relative_path",
                help="Use relative paths as identifiers.",
            ),
            option(
                "--path-like",
                "id_type",
                flag_value="relative_path-like",
                help="Use relative paths as identifiers, match with LIKE.",
            ),
            option(
                "--checksum",
                "id_type",
                flag_value="checksum",
                help="Use checksums as identifiers.",
            ),
            option(
                "--warning",
                "id_type",
                flag_value="warnings",
                help="Use warnings as identifiers.",
            ),
            option(
                "--from-file",
                "id_files",
                is_flag=True,
                default=False,
                help="Interpret IDs as files from which to read the IDs.",
            ),
        ]
        for decorator in reversed(decorators):
            callback = decorator(callback)
        return callback

    return inner


def find_files(database: FileDB, ids: tuple[str, ...], id_type: str, id_files: bool) -> Generator[File, None, None]:
    if id_files:
        ids = tuple(i.strip() for f in ids for i in Path(f).read_text().splitlines() if i.strip())

    if id_type in ("warnings",):
        where: str = f"{id_type} like '%\"' || ? || '\"%'"
    elif id_type.endswith("-like"):
        id_type = id_type.removesuffix("-like")
        where: str = f"{id_type} like ?"
    else:
        where: str = f"{id_type} = ?"

    yield from (file for file_id in ids for file in database.files.select(where=where, parameters=[file_id]))
