from typing import List
from digital_archive.identify.reports import report_results


class TestReportResults:
    """Class for testing the `report_results` function."""

    no_files: List = []
    no_folders: List = []
    files: List[List[str]] = [
        [".txt", "/root/"],
        [".png", "/root/"],
        [".pdf", "/root/"],
    ]
    folders: List[str] = ["/path/to/empty/folder1", "/path/to/empty/folder2"]

    def test_no_files_no_folders(self, tmpdir):
        """`report_results` is invoked with empty files and folders lists.
        No files expected."""
        report_results(self.no_files, self.no_folders, tmpdir)
        assert len(tmpdir.listdir()) == 0

    def test_files_no_folders(self, tmpdir):
        """`report_results` is invoked with a populated file list
        and an empty folders list.
        1 file expected to be written."""
        report_results(self.files, self.no_folders, tmpdir)
        assert len(tmpdir.listdir()) == 1

    def test_files_and_folders(self, tmpdir):
        """`report_results` is invoked with a populated file list
        and a populated folders list.
        2 files expected to be written."""
        report_results(self.files, self.folders, tmpdir)
        assert len(tmpdir.listdir()) == 2
