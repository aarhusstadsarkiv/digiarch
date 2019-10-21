# import pytest
import os
from digital_archive.utils.path_utils import explore_dir
from digital_archive.data import FileInfo


class TestExploreDir:
    """Class for testing the `explore_dir` function."""

    def test_in_empty_dir(self, tmpdir):
        """`explore_dir` is invoked in an empty directory.
        The returned list of FileInfo objects should have len = 0"""

        dir_info = explore_dir(tmpdir)
        assert len(dir_info) == 0

    def test_with_files(self, tmpdir):
        """`explore_dir` is invoked in a non-empty directory,
        with files and non-empty sub-folders.
        The returned list of FileInfo objects should have len = 2,
        the name, ext and path fields in each FileInfo object
        should be populated, and the is_empty_sub field should be False."""

        # Populate `tmpdir` and call `explore_dir(tmpdir)`
        file1 = tmpdir.join("test.txt")
        file2 = tmpdir.mkdir("testdir").join("test.txt")
        file1.write("test")
        file2.write("test")

        file1_info = FileInfo(
            name=file1.basename,
            ext=os.path.splitext(file1)[1].lower(),
            is_empty_sub=False,
            path=file1.dirname,
        )

        file2_info = FileInfo(
            name=file2.basename,
            ext=os.path.splitext(file2)[1].lower(),
            is_empty_sub=False,
            path=file2.dirname,
        )

        dir_info = explore_dir(tmpdir)

        assert len(dir_info) == 2
        assert file1_info in dir_info
        assert file2_info in dir_info

    def test_with_empty_dirs(self, tmpdir):
        """`explore_dir` is invoked in a non-empty directory,
        with no files and empty sub-folders.
        The returned list of FileInfo objects should have len = 1
        and the object should have is_empty_sub=True with a
        populated path field"""

        # Populate `tmpdir` with an empty folder
        # and call `explore_dir(tmpdir)`
        testdir = tmpdir.mkdir("testdir")
        testdir_info = FileInfo(is_empty_sub=True, path=testdir)

        dir_info = explore_dir(tmpdir)

        assert len(dir_info) == 1
        assert testdir_info in dir_info

    def test_with_files_and_empty_dirs(self, tmpdir):
        """`explore_dir` is invoked in a non-empty directory,
        with files and some empty sub-folders.
        The returned list of FileInfo objects should have len = 3,
        with two populated objects and one showing is_empty_sub = True."""

        # Populate tmpdir and call explore_dir(tmpdir)
        file1 = tmpdir.join("test.txt")
        file2 = tmpdir.mkdir("testdir").join("test.txt")
        file1.write("test")
        file2.write("test")
        testdir2 = tmpdir.mkdir("testdir2")

        file1_info = FileInfo(
            name=file1.basename,
            ext=os.path.splitext(file1)[1].lower(),
            is_empty_sub=False,
            path=file1.dirname,
        )

        file2_info = FileInfo(
            name=file2.basename,
            ext=os.path.splitext(file2)[1].lower(),
            is_empty_sub=False,
            path=file2.dirname,
        )
        testdir2_info = FileInfo(is_empty_sub=True, path=testdir2)

        dir_info = explore_dir(tmpdir)

        assert len(dir_info) == 3
        assert file1_info in dir_info
        assert file2_info in dir_info
        assert testdir2_info in dir_info
        # assert len(file_exts) == 2
        # assert len(empty_dirs) == 1
