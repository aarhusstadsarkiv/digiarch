from pathlib import Path

import pytest

from digiarch.internals import FileInfo
from digiarch.utils.group_files import grouping


@pytest.fixture
def test_file_0(temp_dir):
    test_file = Path(temp_dir).joinpath("test0.txt")
    with test_file.open("w") as file:
        file.write("0")
    return test_file


@pytest.fixture
def test_file_1(temp_dir):
    test_file = Path(temp_dir).joinpath("test1.pdf")
    with test_file.open("w") as file:
        file.write("1")
    return test_file


@pytest.fixture
def ignored_file(temp_dir):
    test_file = Path(temp_dir).joinpath("test2.exe")
    with test_file.open("w") as file:
        file.write("1")
    return test_file


class TestGrouping:
    def test_with_file(self, main_dir, test_file_0, test_file_1, ignored_file):
        file_1 = FileInfo(path=test_file_0)
        file_2 = FileInfo(path=test_file_1)
        file_3 = FileInfo(path=ignored_file)
        file_info = [file_1, file_2, file_3]

        grouping(file_info, main_dir)
        contents = [str(p) for p in Path(main_dir / "grouped_files").iterdir()]
        assert any("txt_files.txt" in content for content in contents)
        assert any("pdf_files.txt" in content for content in contents)
        assert any("ignored_files.txt" in content for content in contents)
