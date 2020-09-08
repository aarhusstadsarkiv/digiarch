# -----------------------------------------------------------------------------
# Imports & setup
# -----------------------------------------------------------------------------

import shutil
from datetime import datetime
from typing import List

import pytest
from pydantic import parse_obj_as
from sqlalchemy.exc import OperationalError

from acamodels import ArchiveFile
from digiarch.core import explore_dir
from digiarch.core.identify_files import identify
from digiarch.database import FileDB
from digiarch.models import FileData, Metadata
from freezegun import freeze_time

# -----------------------------------------------------------------------------
# Fixtures
# -----------------------------------------------------------------------------

pytestmark = pytest.mark.asyncio


@pytest.fixture
def files(docx_info, xls_info, adx_info):
    file_list = [{"path": docx_info}, {"path": xls_info}, {"path": adx_info}]
    files = parse_obj_as(List[ArchiveFile], file_list)
    return files


@pytest.fixture
async def db_conn(main_dir):
    file_db = FileDB(f"sqlite:///{main_dir}/test.db")
    await file_db.connect()
    yield file_db
    await file_db.disconnect()


@pytest.fixture
def test_file_data(test_data_dir, db_conn):
    file_data = FileData(main_dir=test_data_dir, db=db_conn, files=[])
    yield file_data
    shutil.rmtree(file_data.data_dir, ignore_errors=True)


# -----------------------------------------------------------------------------
# Tests
# -----------------------------------------------------------------------------


class TestFileDB:
    async def test_exception(self, db_conn, monkeypatch, temp_dir):
        def raise_op_error(*args):
            raise OperationalError("Bad Error", orig=None, params=None)

        def pass_op_error(*args):
            raise OperationalError(
                "IdentificationWarnings",
                orig=None,
                params=None,
            )

        monkeypatch.setattr(FileDB.sql_meta, "create_all", raise_op_error)
        with pytest.raises(OperationalError):
            FileDB(f"sqlite:///{temp_dir}/test.db")

        monkeypatch.setattr(FileDB.sql_meta, "create_all", pass_op_error)
        assert FileDB(f"sqlite:///{temp_dir}/test.db")


class TestMetadata:
    @freeze_time("2012-08-06")
    async def test_set(self, db_conn, test_data_dir, test_file_data):
        file_db = db_conn
        await explore_dir(test_file_data)
        query = file_db.metadata.select()
        result = dict(await file_db.fetch_one(query=query))
        metadata = Metadata(**result)
        print(metadata)
        assert metadata.last_run == datetime(2012, 8, 6, 0, 0)
        assert metadata.processed_dir == test_data_dir
        assert metadata.file_count == 3
        assert metadata.total_size


class TestFiles:
    async def test_insert(self, db_conn, files, test_data_dir):
        file_db = db_conn
        files = identify(files, test_data_dir)
        await file_db.set_files(files=files)
        query = file_db.files.select()
        rows = await file_db.fetch_all(query)
        db_files = parse_obj_as(List[ArchiveFile], rows)
        assert files == db_files

    async def test_get(self, db_conn, files, test_data_dir):
        file_db = db_conn
        files = identify(files, test_data_dir)
        await file_db.set_files(files)
        db_files = await file_db.get_files()
        assert files == db_files

    async def test_is_empty(self, db_conn, files, test_data_dir):
        file_db = db_conn
        assert await file_db.is_empty()
        files = identify(files, test_data_dir)
        await file_db.set_files(files=files)
        assert not await file_db.is_empty()
