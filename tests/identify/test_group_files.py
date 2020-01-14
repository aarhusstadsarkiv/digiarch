import pytest
from pathlib import Path
from digiarch.utils.group_files import grouping
from digiarch.data import FileInfo, to_json


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


class TestGrouping:
    @pytest.mark.xfail
    def test_with_file(self, main_dir, data_file, test_file_0, test_file_1):
        file_1 = FileInfo(path=test_file_0)
        file_2 = FileInfo(path=test_file_1)
        file_info = [file_1, file_2]
        to_json(file_info, data_file)

        grouping(data_file, main_dir)
        contents = [str(p) for p in Path(main_dir).iterdir()]
        assert any("txt_files.txt" in content for content in contents)
        assert any("pdf_files.txt" in content for content in contents)
