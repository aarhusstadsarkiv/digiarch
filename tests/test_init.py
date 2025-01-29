import json
from pathlib import Path
from sqlite3 import Connection
from sqlite3 import Cursor
from sqlite3 import Row

import pytest
from acacore.__version__ import __version__ as acacore_version
from acacore.database import FilesDB
from acacore.models.file import OriginalFile
from click import UsageError
from digiarch.__version__ import __version__ as digiarch_version
from digiarch.cli import app
from digiarch.common import AVID

from .conftest import run_click


def test_init(avid_folder_copy: Path):
    avid = AVID(avid_folder_copy)
    avid.database_path.unlink(missing_ok=True)

    run_click(avid_folder_copy, app, "init", avid.path)

    assert avid.database_path.is_file()

    with FilesDB(avid.database_path, check_initialisation=True, check_version=True) as db:
        events = db.log.select(order_by=[("time", "asc")]).fetchall()
        assert len(events) == 3
        assert events[0].operation == f"{app.name}.init:start"
        assert isinstance(events[0].data, dict)
        # noinspection PyUnresolvedReferences
        assert events[0].data["version"] == digiarch_version
        # noinspection PyUnresolvedReferences
        assert events[0].data["acacore"] == acacore_version
        # noinspection PyUnresolvedReferences
        assert events[0].data["params"]["avid"] == str(avid.path)
        assert events[1].operation == f"{app.name}.init:initialized"
        assert events[1].data == acacore_version
        assert events[2].operation == f"{app.name}.init:end"
        assert events[2].data is None
        assert events[2].reason is None


def test_init_import(avid_folder_copy: Path):
    avid = AVID(avid_folder_copy)
    avid.database_path.unlink(missing_ok=True)
    imported_db_path: Path = avid.dirs.original_documents / "_metadata" / "files.db"

    run_click(avid_folder_copy, app, "init", avid.path, "--import", imported_db_path)

    assert avid.database_path.is_file()

    with FilesDB(avid.database_path, check_initialisation=True, check_version=True) as db:
        imported_db: Connection = Connection(imported_db_path)
        imported_files_cur: Cursor = imported_db.execute("select * from Files")
        imported_files_cur.row_factory = Row
        imported_files: list[Row] = imported_files_cur.fetchall()
        result_files: list[OriginalFile] = db.original_files.select().fetchall()
        imported_db.close()

        assert len(imported_files) == len(result_files)

        for imported_file in imported_files:
            result_file: OriginalFile | None = next(
                (
                    f
                    for f in result_files
                    if str(f.relative_path.relative_to(avid.dirs.original_documents.name))
                    == imported_file["relative_path"]
                ),
                None,
            )
            assert result_file is not None
            assert str(result_file.uuid) == imported_file["uuid"]
            assert result_file.checksum == imported_file["checksum"]
            assert result_file.is_binary == bool(imported_file["is_binary"])
            assert result_file.size == imported_file["size"]
            assert result_file.puid == imported_file["puid"]
            assert result_file.signature == imported_file["signature"]
            assert result_file.warning == json.loads(imported_file["warning"] or "null")
            assert result_file.action == imported_file["action"]
            assert result_file.action_data.model_dump(mode="json") == json.loads(imported_file["action_data"])
            assert result_file.parent == imported_file["parent"]
            assert result_file.processed == bool(imported_file["processed"])
            assert result_file.lock == bool(imported_file["lock"])
            assert (
                str(result_file.original_path.relative_to(avid.dirs.original_documents.name))
                == imported_file["original_path"]
            )


def test_init_invalid(avid_folder_copy: Path):
    avid = AVID(avid_folder_copy)

    with pytest.raises(UsageError, match=f"{str(avid.metadata_dir)!r} is not a valid AVID directory."):
        run_click(avid.metadata_dir, app, "init", avid.metadata_dir)
