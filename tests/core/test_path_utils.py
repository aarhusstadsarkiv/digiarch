# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------

from digiarch.models.file_data import FileData
from uuid import uuid4

# import acamodels.archive_file. Replaced by line below.
import digiarch.core.ArchiveFileRel
import pytest
from digiarch.core.ArchiveFileRel import ArchiveFile

from digiarch.core.path_utils import explore_dir
from digiarch.exceptions import FileCollectionError
import os
from pathlib import Path
import shutil

# -----------------------------------------------------------------------------
# Fixtures
# -----------------------------------------------------------------------------

pytestmark = pytest.mark.asyncio


@pytest.fixture
def test_dir():
    test_dir: Path = Path.cwd() / "testdir"
    test_dir.mkdir()
    return test_dir


# -----------------------------------------------------------------------------
# Tests
# -----------------------------------------------------------------------------


class TestExploreDir:
    """Class for testing the `explore_dir` function."""

    async def test_in_empty_dir(self, file_data):
        """`explore_dir` is invoked in an empty directory.
        The data file should be empty."""
        with pytest.raises(FileCollectionError):
            await explore_dir(file_data)

    async def test_with_files(self, test_dir: Path, monkeypatch):
        """explore_dir is invoked in a non-empty directory,
        with files and non-empty sub-folders.
        The resulting JSON file should be populated,
        and we should be able to reconstruct file infos."""

        # Set the ROOTPATH environment variable for explore_dir.
        os.environ["ROOTPATH"] = str(test_dir)
        # Populate temp_dir and define file info
        file1: Path = test_dir / "new_test_file.txt"
        file2: Path = test_dir / "test" / "test.txt"
        file1.touch()
        file2.parent.mkdir()
        file2.touch()
        file1.write_text("test")
        file2.write_text("test")

        print("File 1: {}".format(str(file1)))
        print("File 2: {}".format(str(file2)))
        print("Root Path: {}".format(os.environ["ROOTPATH"]))

        # Patch uuid
        static_uuid = uuid4()

        def uuid_return():
            return static_uuid

        monkeypatch.setattr(
            digiarch.core.ArchiveFileRel,
            "uuid4",
            uuid_return,
        )

        print("file1_rel: {}".format(str(file1.relative_to(test_dir))))
        print("file2_rel: {}".format(str(file2.relative_to(test_dir))))

        # Since files from db.get_files() contains relative paths,
        # the paths are stored as relative in file1_info and file2_info.
        file1_info = ArchiveFile(relative_path=file1.relative_to(test_dir))
        file2_info = ArchiveFile(relative_path=file2.relative_to(test_dir))
        file_data = FileData(main_dir=test_dir, files=[])
        await explore_dir(file_data)
        files = await file_data.db.get_files()
        assert len(files) == 2
        assert file1_info in files
        assert file2_info in files

        # Make file collection fail
        def fail(*args, **kwargs):
            raise Exception("Oh no")

        monkeypatch.setattr(ArchiveFile, "__init__", fail)
        with pytest.raises(FileCollectionError, match="Oh no"):
            await explore_dir(file_data)

        shutil.rmtree(test_dir)
