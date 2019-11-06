import os
from digiarch.utils.path_utils import explore_dir
from digiarch.data import FileInfo, get_fileinfo_list


class TestExploreDir:
    """Class for testing the `explore_dir` function."""

    def test_in_empty_dir(self, temp_dir, main_dir, data_file):
        """`explore_dir` is invoked in an empty directory.
        The data file should be empty."""
        explore_dir(temp_dir, main_dir, data_file)
        result = get_fileinfo_list(data_file)
        assert len(result) == 0

    def test_with_files(self, temp_dir, main_dir, data_file):
        """explore_dir is invoked in a non-empty directory,
        with files and non-empty sub-folders.
        The resulting JSON file should be populated,
        and we should be able to reconstruct file infos."""

        # Populate temp_dir and define file info
        file1 = temp_dir.join("test.txt")
        file2 = temp_dir.mkdir("testdir").join("test.txt")
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

        explore_dir(temp_dir, main_dir, data_file)
        result = get_fileinfo_list(data_file)

        assert len(result) == 2
        assert file1_info in result
        assert file2_info in result

    def test_with_empty_dirs(self, temp_dir, main_dir, data_file):
        """`explore_dir` is invoked in a non-empty directory,
        with no files and empty sub-folders.
        The returned list of FileInfo objects should have len = 1
        and the object should have is_empty_sub=True with a
        populated path field"""

        # Populate temp_dir with an empty folder
        testdir2 = temp_dir.mkdir("testdir2")
        testdir2_info = FileInfo(is_empty_sub=True, path=testdir2)

        explore_dir(temp_dir, main_dir, data_file)
        result = get_fileinfo_list(data_file)

        assert len(result) == 1
        assert testdir2_info in result

    def test_with_files_and_empty_dirs(self, temp_dir, main_dir, data_file):
        """`explore_dir` is invoked in a non-empty directory,
        with files and some empty sub-folders.
        The returned list of FileInfo objects should have len = 3,
        with two populated objects and one showing is_empty_sub = True."""

        # Populate tmpdir and call explore_dir(tmpdir)
        file1 = temp_dir.join("test.txt")
        file2 = temp_dir.mkdir("testdir").join("test.txt")
        file1.write("test")
        file2.write("test")
        testdir2 = temp_dir.mkdir("testdir2")

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

        explore_dir(temp_dir, main_dir, data_file)
        result = get_fileinfo_list(data_file)

        assert len(result) == 3
        assert file1_info in result
        assert file2_info in result
        assert testdir2_info in result
        # assert len(file_exts) == 2
        # assert len(empty_dirs) == 1
