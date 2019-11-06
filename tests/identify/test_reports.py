import json
import os
from typing import List
from digiarch.identify.reports import report_results
from digiarch.data import FileInfo, encode_dataclass


def write_test_file(temp_dir, data_file, dir_info):
    with open(data_file, "w") as file:
        file.write(json.dumps(dir_info, default=encode_dataclass, indent=4))


class TestReportResults:
    """Class for testing the `report_results` function."""

    file_info = FileInfo(
        name="test.txt", ext=".txt", is_empty_sub=False, path="/root/"
    )
    empty_sub_info = FileInfo(is_empty_sub=True, path="/root/")
    no_dir_info: List = []
    dir_info_file = [file_info]
    dir_info_file_empty_sub = [file_info, empty_sub_info]

    def test_no_files_no_folders(self, temp_dir, data_file):
        """`report_results` is invoked with an empty list of FileInfo objects.
        No files expected."""

        write_test_file(temp_dir, data_file, self.no_dir_info)
        report_results(data_file, temp_dir)
        files_in_temp = [
            file
            for file in os.listdir(temp_dir)
            if os.path.isfile(os.path.join(temp_dir, file))
        ]
        assert len(files_in_temp) == 0

    def test_files_no_folders(self, temp_dir, data_file):
        """`report_results` is invoked with a populated list of
        FileInfo objects. None of them are empty subfolders.
        1 file expected to be written."""
        write_test_file(temp_dir, data_file, self.dir_info_file)
        report_results(data_file, temp_dir)
        files_in_temp = [
            file
            for file in os.listdir(temp_dir)
            if os.path.isfile(os.path.join(temp_dir, file))
        ]
        assert len(files_in_temp) == 1

    def test_files_and_folders(self, temp_dir, data_file):
        """`report_results` is invoked with a populated list of
        FileInfo objects. One is a file, one is an empty subfolder.
        2 files expected to be written."""
        write_test_file(temp_dir, data_file, self.dir_info_file_empty_sub)
        report_results(data_file, temp_dir)
        files_in_temp = [
            file
            for file in os.listdir(temp_dir)
            if os.path.isfile(os.path.join(temp_dir, file))
        ]
        assert len(files_in_temp) == 2
