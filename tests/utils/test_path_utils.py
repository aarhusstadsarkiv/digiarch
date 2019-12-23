from pathlib import Path
from digiarch.utils.path_utils import explore_dir
from digiarch.data import FileInfo, get_fileinfo_list


class TestExploreDir:
    """Class for testing the `explore_dir` function."""

    def test_in_empty_dir(self, temp_dir, main_dir, data_file):
        """`explore_dir` is invoked in an empty directory.
        The data file should be empty."""
        explore_dir(Path(temp_dir), Path(main_dir), data_file)
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
            name=Path(file1).name,
            ext=Path(file1).suffix.lower(),
            path=Path(file1.dirname, file1.basename),
        )

        file2_info = FileInfo(
            name=Path(file2).name,
            ext=Path(file2).suffix.lower(),
            path=Path(file2.dirname, file2.basename),
        )

        explore_dir(Path(temp_dir), Path(main_dir), data_file)
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

        result = explore_dir(Path(temp_dir), Path(main_dir), data_file)

        assert len(result) == 1
        assert testdir2 in result
