import json
import pytest
from digiarch.data import FileInfo, DataJSONEncoder


@pytest.fixture
def file_info():
    return FileInfo(name="test.txt", ext=".txt", path="/root/test.txt")


class FailJSON:
    def func(self):
        pass


@pytest.fixture
def make_json_fail():
    return FailJSON


class TestFileInfo:
    def test_init(self):
        f_info = FileInfo()
        assert f_info.name == ""
        assert f_info.ext == ""
        assert f_info.path == ""
        assert f_info.checksum == ""
        f2_info = FileInfo("test.txt", ".txt")
        assert f2_info.name == "test.txt"
        assert f2_info.ext == ".txt"

    def test_to_dict(self, file_info):
        dict_info = file_info.to_dict()
        assert dict_info["name"] == "test.txt"
        assert dict_info["ext"] == ".txt"
        assert dict_info["path"] == "/root/test.txt"

    def test_replace(self, file_info):
        assert file_info.name == "test.txt"
        assert file_info.path == "/root/test.txt"
        new_info = file_info.replace(name="new_name")
        assert new_info.name == "new_name"
        assert new_info.ext == file_info.ext
        assert new_info.path == file_info.path

    def test_from_dict(self):
        dict_info = {"name": "dict_test"}
        file_info = FileInfo.from_dict(dict_info)
        assert file_info.name == dict_info["name"]


class TestJSONEncode:
    def test_with_valid_input(self):
        data = json.dumps(123, cls=DataJSONEncoder)
        assert json.loads(data) == 123

    def test_with_invalid_input(self, make_json_fail):
        with pytest.raises(TypeError):
            json.dumps(make_json_fail, cls=DataJSONEncoder)
