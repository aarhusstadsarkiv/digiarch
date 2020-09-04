# -----------------------------------------------------------------------------
# Imports & setup
# -----------------------------------------------------------------------------

from datetime import datetime
from typing import List

import pytest
from pydantic import parse_obj_as
from sqlalchemy.exc import OperationalError
from freezegun import freeze_time

from digiarch.utils.path_utils import explore_dir
from digiarch.internals import ArchiveFile, Metadata
from digiarch.database import FileDB
from digiarch.identify.identify_files import identify

# -----------------------------------------------------------------------------
# Fixtures
# -----------------------------------------------------------------------------

pytestmark = pytest.mark.asyncio


@pytest.fixture
def files(docx_info, xls_info, adx_info):
    file_list = [{"path": docx_info}, {"path": xls_info}, {"path": adx_info}]
    files = parse_obj_as(List[ArchiveFile], file_list)
    return files


class MockMetaData:
    @staticmethod
    def create_all():
        raise OperationalError("test")


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
    async def test_set(self, db_conn, test_data_dir):
        file_db = db_conn
        await explore_dir(test_data_dir, file_db)
        # metadata = file_data.metadata
        # await file_db.set_metadata(metadata)
        query = file_db.metadata.select()
        result = dict(await file_db.fetch_one(query=query))
        metadata = Metadata(**result)
        print(metadata)
        assert metadata.last_run == datetime(2012, 8, 6, 0, 0)
        assert metadata.processed_dir == test_data_dir
        assert metadata.file_count == 3
        assert metadata.total_size == "61.8 KiB"


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
