import pytest
from digital_archive.file_stats import (
    create_parser,
    explore_dir,
    report_results,
    main,
)


@pytest.fixture
def parser():
    return create_parser()


class TestCreateArgs:
    """Class for testing the `create_args()` function."""

    def test_empty_input(self, parser):
        """User inputs no arguments.
        Should fail with `SystemExit`"""

        with pytest.raises(SystemExit):
            parser.parse_args([])

    def test_not_a_path(self, parser, tmpdir):
        """User input is not a directory.
        Should fail with NotADirectory Error."""

        with pytest.raises(NotADirectoryError):
            parser.parse_args(["this_is_not_a_path"])

    def test_is_a_path(self, parser, tmpdir):
        """User input is a directory.
        Should pass with no exceptions."""

        parser.parse_args([str(tmpdir)])


class TestExploreDir:
    """Class for testing the `explore_dir()` function."""

    def test_in_empty_dir(self, tmpdir):
        """explore_dir() is invoked in an empty directory.
        Return values file_exts and empty_dirs should both have len=0"""

        file_exts, empty_dirs = explore_dir(tmpdir)
        assert len(file_exts) == 0
        assert len(empty_dirs) == 0

    def test_with_files(self, tmpdir):
        """explore_dir() is invoked in a non-empty directory,
        with files and non-empty sub-folders.
        Return value file_exts should be populated,
        while empty_dirs should be empty."""

        # Populate tmpdir and call explore_dir(tmpdir)
        file1 = tmpdir.join("test.txt")
        file2 = tmpdir.mkdir("testdir").join("test.txt")
        file1.write("test")
        file2.write("test")

        file_exts, empty_dirs = explore_dir(tmpdir)

        # file_exts should have len = 2
        # empty_dirs should have len = 0
        assert len(file_exts) == 2
        assert len(empty_dirs) == 0

    def test_with_empty_dirs(self, tmpdir):
        """explore_dir() is invoked in a non-empty directory,
        with no files and empty sub-folders.
        Return value file_exts should be empty,
        while empty_dirs should be populated."""

        # Populate tmpdir with an empty folder
        # and call explore_dir(tmpdir)
        tmpdir.mkdir("testdir")

        file_exts, empty_dirs = explore_dir(tmpdir)

        # file_exts should have len = 0
        # empty_dirs should have len = 1
        assert len(file_exts) == 0
        assert len(empty_dirs) == 1

    def test_with_files_and_empty_dirs(self, tmpdir):
        """explore_dir() is invoked in a non-empty directory,
        with files and some empty sub-folders.
        Both return values should be populated"""

        # Populate tmpdir and call explore_dir(tmpdir)
        file1 = tmpdir.join("test.txt")
        file2 = tmpdir.mkdir("testdir").join("test.txt")
        file1.write("test")
        file2.write("test")
        tmpdir.mkdir("testdir2")

        file_exts, empty_dirs = explore_dir(tmpdir)

        # file_exts should have len = 2
        # empty_dirs should have len = 1
        assert len(file_exts) == 2
        assert len(empty_dirs) == 1


class TestReportResults:
    """Class for testing the `report_results()` function."""

    no_files = []
    no_folders = []
    files = [[".txt", "/root"], [".png", "/root"], [".pdf", "/root/"]]
    folders = ["/path/to/empty/folder1", "/path/to/empty/folder2"]

    def test_no_files_no_folders(self, tmpdir):
        result = report_results(self.no_files, self.no_folders, tmpdir)
        # assert result

    def test_files_no_folders(self, tmpdir):
        result = report_results(self.files, self.no_folders, tmpdir)
        assert result is None

    def test_files_and_folders(self, tmpdir):
        result = report_results(self.files, self.folders, tmpdir)
        assert result is None


class TestMain:
    """Class for testing the `main()` function."""

    def test_main(self, parser, tmpdir):
        args = parser.parse_args([str(tmpdir)])
        result = main(args)
        assert result is None
