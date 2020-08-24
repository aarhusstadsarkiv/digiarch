# -----------------------------------------------------------------------------
# Imports & setup
# -----------------------------------------------------------------------------

from pathlib import Path
import pytest

from digiarch.database import FileDB
from digiarch.identify.identify_files import identify
from acamodels import ArchiveFile

pytestmark = pytest.mark.asyncio


@pytest.fixture
def docx_info(test_data_dir):
    docx_file: Path = test_data_dir / "docx_test.docx"
    return docx_file


# -----------------------------------------------------------------------------
# Tests
# -----------------------------------------------------------------------------


class TestFileDB:
    async def test_connect(self, temp_dir, docx_info):
        file_db = FileDB("sqlite:////home/jnik/data/test.db")
        await file_db.connect()
        files = [ArchiveFile(path=docx_info)]
        files = identify(files, docx_info.parent)
        await file_db.insert_files(files=files)
        await file_db.get_files()
        assert False
