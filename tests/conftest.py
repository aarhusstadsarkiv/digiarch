"""Shared testing fixtures.

"""

# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------

import inspect
from datetime import datetime
from pathlib import Path

import pytest

from digiarch.database import FileDB
from digiarch.internals import FileData, Metadata

# -----------------------------------------------------------------------------
# Function Definitions
# -----------------------------------------------------------------------------

pytestmark = pytest.mark.asyncio


@pytest.fixture
def temp_dir(tmpdir_factory):
    temp_dir: str = tmpdir_factory.mktemp("temp_dir")
    return Path(temp_dir)


@pytest.fixture
def main_dir(temp_dir):
    main_dir: Path = temp_dir / "_digiarch"
    main_dir.mkdir(exist_ok=True)
    return main_dir


@pytest.fixture
def data_file(main_dir):
    data_dir: Path = main_dir / ".data"
    data_dir.mkdir(exist_ok=True)
    data_file: Path = data_dir / "data.json"
    return data_file


@pytest.fixture
def file_data(temp_dir):
    cur_time = datetime.now()
    metadata = Metadata(last_run=cur_time, processed_dir=Path(temp_dir))
    return FileData(metadata=metadata)


@pytest.fixture
def test_data_dir():
    return Path(inspect.getfile(FileData)).parent.parent / "tests" / "_data"


@pytest.fixture
def docx_info(test_data_dir):
    docx_file: Path = test_data_dir / "docx_test.docx"
    return docx_file


@pytest.fixture
def xls_info(test_data_dir):
    xls_file: Path = test_data_dir / "xls_test.xls"
    return xls_file


@pytest.fixture
def adx_info(test_data_dir):
    adx_file: Path = test_data_dir / "adx_test.adx"
    return adx_file


@pytest.fixture
async def db_conn(main_dir):
    file_db = FileDB(f"sqlite:///{main_dir}/test.db")
    await file_db.connect()
    yield file_db
    await file_db.disconnect()
