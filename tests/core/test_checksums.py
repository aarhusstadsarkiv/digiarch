# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------
from pathlib import Path

import digiarch.core.checksums as check
import pytest
from digiarch.core.ArchiveFileRel import ArchiveFile

# -----------------------------------------------------------------------------
# Fixtures
# -----------------------------------------------------------------------------


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
    return ArchiveFile(relative_path=test_file_0)


# -----------------------------------------------------------------------------
# Tests
# -----------------------------------------------------------------------------


class TestFileChecksum:
    def test_without_file(self, temp_dir):
        assert check.file_checksum(Path(temp_dir)) == ""

    def test_with_file(self, test_file_0):
        assert (
            check.file_checksum(test_file_0)
            == "5feceb66ffc86f38d952786c6d696c79"
            "c2dbc239dd4e91b46729d73a27fb57e9"
        )


class TestChecksumWorker:
    def test_with_file(self, test_file_info):
        assert (
            check.checksum_worker(test_file_info).checksum
            == "5feceb66ffc86f38d952786c6d696c79"
            "c2dbc239dd4e91b46729d73a27fb57e9"
        )


class TestGenerateChecksums:
    def test_with_files(self, test_file_0, test_file_1):
        f_info_0 = ArchiveFile(relative_path=test_file_0, checksum="test0")
        f_info_1 = ArchiveFile(relative_path=test_file_1, checksum="test1")
        files = [f_info_0, f_info_1]
        result = check.generate_checksums(files)
        for file in result:
            assert file.checksum == check.file_checksum(file.relative_path)

    def test_without_files(self):
        assert check.generate_checksums([]) == []

    def test_exception(self, test_file_0, monkeypatch):
        f_info_0 = ArchiveFile(path=test_file_0, checksum="test0")
        files = [f_info_0] * 10

        def mock_err(*args):
            raise Exception("Bad Error")

        # This monkeypatching makes imap_unordered fail rather horribly :)
        monkeypatch.setattr(check, "checksum_worker", mock_err)
        with pytest.raises(Exception):
            check.generate_checksums(files)


class TestCheckCollisions:
    def test_with_collisions(self):
        item1 = "collision"
        item2 = "not a collision"
        test_list = [item1, item1, item2]
        result = check.check_collisions(test_list)
        assert item1 in result
        assert item2 not in result
        assert len(result) == 1

    def test_without_collisions(self):
        item1 = "not a collision"
        item2 = "also not a collision"
        test_list = [item1, item2]
        result = check.check_collisions(test_list)
        assert item1 not in result
        assert item2 not in result
        assert len(result) == 0


# class TestCheckDuplicates:
#     def test_with_dups(self, test_file_0, temp_dir):
#         f_info_0 = ArchiveFile(path=test_file_0, checksum="test0")
#         f_info_1 = ArchiveFile(path=test_file_0, checksum="test1")
#         files = [f_info_0, f_info_1]
#         updated_files = check.generate_checksums(files)
#         print(updated_files)
#         check.check_duplicates(updated_files, temp_dir)
#         outfile = Path(temp_dir).joinpath("duplicate_files.json")
#         with outfile.open() as file:
#             result = json.load(file)
#         assert check.file_checksum(test_file_0) in result
#         assert len(result[check.file_checksum(test_file_0)]) == 2

#     def test_without_dups(self, test_file_0, test_file_1, temp_dir):
#         f_info_0 = ArchiveFile(path=test_file_0, checksum="test0")
#         f_info_1 = ArchiveFile(path=test_file_1, checksum="test1")
#         files = [f_info_0, f_info_1]
#         updated_files = check.generate_checksums(files)
#         check.check_duplicates(updated_files, temp_dir)
#         outfile = Path(temp_dir).joinpath("duplicate_files.json")
#         with outfile.open() as file:
#             result = json.load(file)
#         assert check.file_checksum(test_file_0) not in result
#         assert check.file_checksum(test_file_1) not in result
#         assert result == {}
