# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------
from uuid import uuid4

# import acamodels.archive_file. Replaced by line below.
import digiarch.core.ArchiveFileRel
import pytest
from digiarch.core.ArchiveFileRel import ArchiveFile

from digiarch.core.path_utils import explore_dir
from digiarch.exceptions import FileCollectionError
import os
from pathlib import Path

# -----------------------------------------------------------------------------
# Fixtures
# -----------------------------------------------------------------------------

pytestmark = pytest.mark.asyncio


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

    async def test_with_files(self, temp_dir, monkeypatch, file_data):
        """explore_dir is invoked in a non-empty directory,
        with files and non-empty sub-folders.
        The resulting JSON file should be populated,
        and we should be able to reconstruct file infos."""

        # Set the ROOTPATH environment variable for explore_dir.
        os.environ["ROOTPATH"] = str(temp_dir)
        # Populate temp_dir and define file info
        file1: Path = temp_dir / "test.txt"
        file2: Path = temp_dir / "testdir" / "test.txt"
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

        # Since files from db.get_files() contains relative paths,
        # the paths are stored as relative in file1_info and file2_info.
        file1_info = ArchiveFile(relative_path=file1.relative_to(temp_dir))
        file2_info = ArchiveFile(relative_path=file2.relative_to(temp_dir))

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
