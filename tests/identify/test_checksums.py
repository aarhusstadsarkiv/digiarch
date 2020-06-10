import json
from pathlib import Path
from unittest.mock import patch

import pytest

from digiarch.identify.checksums import (check_collisions, check_duplicates,
                                         checksum_worker, file_checksum,
                                         generate_checksums)
from digiarch.internals import FileInfo


@pytest.fixture
def test_file_0(temp_dir):
    test_file = Path(temp_dir).joinpath("test0.txt")
    with test_file.open("w") as file:
        file.write("0")
    return test_file


@pytest.fixture
def test_file_1(temp_dir):
    test_file = Path(temp_dir).joinpath("test1.txt")
    with test_file.open("w") as file:
        file.write("1")
    return test_file


@pytest.fixture
def test_file_info(test_file_0):
    return FileInfo(path=test_file_0)


class TestFileChecksum:
    def test_without_file(self, temp_dir):
        assert file_checksum(Path(temp_dir)) == ""

    def test_with_file(self, test_file_0):
        assert (
            file_checksum(test_file_0) == "5feceb66ffc86f38d952786c6d696c79"
            "c2dbc239dd4e91b46729d73a27fb57e9"
        )


class TestChecksumWorker:
    def test_without_file(self, temp_dir):
        file_info = FileInfo(path=temp_dir)
        result = checksum_worker(file_info)
        assert result.checksum is None

    def test_with_file(self, test_file_info):
        assert (
            checksum_worker(test_file_info).checksum
            == "5feceb66ffc86f38d952786c6d696c79"
            "c2dbc239dd4e91b46729d73a27fb57e9"
        )


class TestGenerateChecksums:
    def test_with_files(self, test_file_0, test_file_1):
        f_info_0 = FileInfo(path=test_file_0, checksum="test0")
        f_info_1 = FileInfo(path=test_file_1, checksum="test1")
        files = [f_info_0, f_info_1]
        result = generate_checksums(files)
        for file in result:
            assert file.checksum == file_checksum(Path(file.path))

    def test_without_files(self):
        assert generate_checksums([]) == []

    def test_exception(self, test_file_0):
        f_info_0 = FileInfo(path=test_file_0, checksum="test0")
        files = [f_info_0] * 10
        # Raise an arbitrary exception, test that it bubbles up
        with pytest.raises(ValueError):
            with patch(
                "digiarch.identify.checksums.file_checksum",
                side_effect=ValueError,
            ):
                generate_checksums(files)


class TestCheckCollisions:
    def test_with_collisions(self):
        item1 = "collision"
        item2 = "not a collision"
        test_list = [item1, item1, item2]
        result = check_collisions(test_list)
        assert item1 in result
        assert item2 not in result
        assert len(result) == 1

    def test_without_collisions(self):
        item1 = "not a collision"
        item2 = "also not a collision"
        test_list = [item1, item2]
        result = check_collisions(test_list)
        assert item1 not in result
        assert item2 not in result
        assert len(result) == 0


class TestCheckDuplicates:
    def test_with_dups(self, test_file_0, temp_dir):
        f_info_0 = FileInfo(path=test_file_0, checksum="test0")
        f_info_1 = FileInfo(path=test_file_0, checksum="test1")
        files = [f_info_0, f_info_1]
        updated_files = generate_checksums(files)
        print(updated_files)
        check_duplicates(updated_files, temp_dir)
        outfile = Path(temp_dir).joinpath("duplicate_files.json")
        with outfile.open() as file:
            result = json.load(file)
        assert file_checksum(test_file_0) in result
        assert len(result[file_checksum(test_file_0)]) == 2

    def test_without_dups(self, test_file_0, test_file_1, temp_dir):
        f_info_0 = FileInfo(path=test_file_0, checksum="test0")
        f_info_1 = FileInfo(path=test_file_1, checksum="test1")
        files = [f_info_0, f_info_1]
        updated_files = generate_checksums(files)
        check_duplicates(updated_files, temp_dir)
        outfile = Path(temp_dir).joinpath("duplicate_files.json")
        with outfile.open() as file:
            result = json.load(file)
        assert file_checksum(test_file_0) not in result
        assert file_checksum(test_file_1) not in result
        assert result == {}
