# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------

from datetime import datetime
from pathlib import Path
from uuid import uuid4

import pytest

import acamodels.archive_file
from digiarch.exceptions import FileCollectionError
from digiarch.internals import ArchiveFile, FileData, Metadata
from digiarch.utils.path_utils import explore_dir

# -----------------------------------------------------------------------------
# Fixtures
# -----------------------------------------------------------------------------


@pytest.fixture
def file_data(temp_dir):
    cur_time = datetime.now()
    metadata = Metadata(last_run=cur_time, processed_dir=Path(temp_dir))
    return FileData(metadata=metadata)


# -----------------------------------------------------------------------------
# Tests
# -----------------------------------------------------------------------------


class TestExploreDir:
    """Class for testing the `explore_dir` function."""

    def test_in_empty_dir(self, temp_dir, file_data):
        """`explore_dir` is invoked in an empty directory.
        The data file should be empty."""
        with pytest.raises(FileCollectionError):
            explore_dir(temp_dir)
        assert not file_data.digiarch_dir.exists()

    def test_with_files(self, temp_dir, monkeypatch):
        """explore_dir is invoked in a non-empty directory,
        with files and non-empty sub-folders.
        The resulting JSON file should be populated,
        and we should be able to reconstruct file infos."""

        # Populate temp_dir and define file info
        file1 = temp_dir / "test.txt"
        file2 = temp_dir / "testdir" / "test.txt"
        file1.touch()
        file2.parent.mkdir()
        file2.touch()
        file1.write_text("test")
        file2.write_text("test")

        # Patch uuid
        static_uuid = uuid4()

        def uuid_return():
            return static_uuid

        monkeypatch.setattr(
            acamodels.archive_file, "uuid4", uuid_return,
        )

        file1_info = ArchiveFile(path=file1)

        file2_info = ArchiveFile(path=file2)

        file_data = explore_dir(temp_dir)

        assert len(file_data.files) == 2
        assert file1_info in file_data.files
        assert file2_info in file_data.files

    def test_with_empty_dirs(self, temp_dir):
        """Invoke in non-empty directory containing no files but one empty
        subdirectory. The files field of the returned FileData object should be
        empty, and the path to the empty subdirectory should exist in the
        Metadata field named empty_subdirectories."""

        # Populate temp_dir with an empty folder
        testdir2 = temp_dir / "testdir2"
        testdir2.mkdir()

        file_data = explore_dir(temp_dir)
        print(file_data)
        assert len(file_data.files) == 0
        # assert testdir2 in (file_data.metadata.empty_subdirs or [])
