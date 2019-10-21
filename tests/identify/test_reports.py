from typing import List
from digital_archive.identify.reports import report_results
from digital_archive.data import FileInfo


class TestReportResults:
    """Class for testing the `report_results` function."""

    no_dir_info: List = []
    file_info = FileInfo(
        name="test.txt", ext=".txt", is_empty_sub=False, path="/root/"
    )
    empty_sub_info = FileInfo(is_empty_sub=True, path="/root/")
    dir_info_file = [file_info]
    dir_info_file_empty_sub = [file_info, empty_sub_info]

    def test_no_files_no_folders(self, tmpdir):
        """`report_results` is invoked with an empty list of FileInfo objects.
        No files expected."""
        report_results(self.no_dir_info, tmpdir)
        assert len(tmpdir.listdir()) == 0

    def test_files_no_folders(self, tmpdir):
        """`report_results` is invoked with a populated list of
        FileInfo objects. None of them are empty subfolders.
        1 file expected to be written."""
        report_results(self.dir_info_file, tmpdir)
        assert len(tmpdir.listdir()) == 1

    def test_files_and_folders(self, tmpdir):
        """`report_results` is invoked with a populated list of
        FileInfo objects. One is a file, one is an empty subfolder.
        2 files expected to be written."""
        report_results(self.dir_info_file_empty_sub, tmpdir)
        assert len(tmpdir.listdir()) == 2
