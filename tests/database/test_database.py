# -----------------------------------------------------------------------------
# Imports & setup
# -----------------------------------------------------------------------------

from typing import List

import pytest
from pydantic import parse_obj_as

from digiarch.database import FileDB
from digiarch.identify.identify_files import identify
from acamodels import ArchiveFile


# -----------------------------------------------------------------------------
# Fixtures
# -----------------------------------------------------------------------------

pytestmark = pytest.mark.asyncio


@pytest.fixture
async def db_conn(temp_dir):
    from pathlib import Path

    print(Path.home())
    file_db = FileDB(f"sqlite:///{temp_dir}/test.db")
    await file_db.connect()
    yield file_db
    await file_db.disconnect()


@pytest.fixture
def files(docx_info, xls_info, adx_info):
    file_list = [{"path": docx_info}, {"path": xls_info}, {"path": adx_info}]
    files = parse_obj_as(List[ArchiveFile], file_list)
    return files


# -----------------------------------------------------------------------------
# Tests
# -----------------------------------------------------------------------------


class TestFileDB:
    async def test_insert(self, db_conn, files, test_data_dir):
        file_db = db_conn
        files = identify(files, test_data_dir)
        await file_db.insert_files(files=files)
        query = file_db.files.select()
        rows = await file_db.fetch_all(query)
        db_files = parse_obj_as(List[ArchiveFile], rows)
        assert files == db_files

    async def test_get(self, db_conn, files, test_data_dir):
        file_db = db_conn
        files = identify(files, test_data_dir)
        await file_db.insert_files(files)
        db_files = await file_db.get_files()
        assert files == db_files

    async def test_update(self, db_conn, files, test_data_dir):
        file_db = db_conn
        await file_db.insert_files(files)
        db_files = await file_db.get_files()
        updated_file = files[0].copy(update={"checksum": "test123"})
        await file_db.update_files([updated_file])
        db_files = await file_db.get_files()
        assert updated_file in db_files
