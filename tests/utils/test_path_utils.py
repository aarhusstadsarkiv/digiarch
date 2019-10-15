# import pytest
from digital_archive.utils.path_utils import explore_dir


class TestExploreDir:
    """Class for testing the `explore_dir` function."""

    def test_in_empty_dir(self, tmpdir):
        """`explore_dir` is invoked in an empty directory.
        Return values `file_exts` and `empty_dirs` should both have len=0"""

        file_exts, empty_dirs = explore_dir(tmpdir)
        assert len(file_exts) == 0
        assert len(empty_dirs) == 0

    def test_with_files(self, tmpdir):
        """`explore_dir` is invoked in a non-empty directory,
        with files and non-empty sub-folders.
        Return value `file_exts` should be populated,
        while `empty_dirs` should be empty."""

        # Populate `tmpdir` and call `explore_dir(tmpdir)`
        file1 = tmpdir.join("test.txt")
        file2 = tmpdir.mkdir("testdir").join("test.txt")
        file1.write("test")
        file2.write("test")

        file_exts, empty_dirs = explore_dir(tmpdir)

        # `file_exts` should have len = 2
        # `empty_dirs` should have len = 0
        assert len(file_exts) == 2
        assert len(empty_dirs) == 0

    def test_with_empty_dirs(self, tmpdir):
        """`explore_dir` is invoked in a non-empty directory,
        with no files and empty sub-folders.
        Return value `file_exts` should be empty,
        while `empty_dirs` should be populated."""

        # Populate `tmpdir` with an empty folder
        # and call `explore_dir(tmpdir)`
        tmpdir.mkdir("testdir")

        file_exts, empty_dirs = explore_dir(tmpdir)

        # `file_exts` should have `len = 0`
        # `empty_dirs` should have `len = 1`
        assert len(file_exts) == 0
        assert len(empty_dirs) == 1

    def test_with_files_and_empty_dirs(self, tmpdir):
        """`explore_dir` is invoked in a non-empty directory,
        with files and some empty sub-folders.
        Both return values should be populated"""

        # Populate `tmpdir` and call `explore_dir(tmpdir)`
        file1 = tmpdir.join("test.txt")
        file2 = tmpdir.mkdir("testdir").join("test.txt")
        file1.write("test")
        file2.write("test")
        tmpdir.mkdir("testdir2")

        file_exts, empty_dirs = explore_dir(tmpdir)

        # `file_exts` should have `len = 2`
        # `empty_dirs` should have `len = 1`
        assert len(file_exts) == 2
        assert len(empty_dirs) == 1
