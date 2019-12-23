import json
import pytest
from pathlib import Path
from digiarch.data import FileInfo, DataJSONEncoder
from dacite import MissingValueError


@pytest.fixture
def file_info():
    return FileInfo(name="test.txt", ext=".txt", path=Path("/root/test.txt"))


class FailJSON:
    def func(self):
        pass


@pytest.fixture
def make_json_fail():
    return FailJSON


class TestFileInfo:
    def test_init(self):
        # We cannot create an empty FileInfo.
        with pytest.raises(TypeError):
            FileInfo()
        f_info = FileInfo(name="test.txt", ext=".txt", path=Path("/"))
        assert f_info.name == "test.txt"
        assert f_info.ext == ".txt"
        assert f_info.path == Path("/")
        assert f_info.checksum is None
        assert f_info.identification is None

    def test_to_dict(self, file_info):
        dict_info = file_info.to_dict()
        assert dict_info["name"] == "test.txt"
        assert dict_info["ext"] == ".txt"
        assert dict_info["path"] == Path("/root/test.txt")

    def test_replace(self, file_info):
        assert file_info.name == "test.txt"
        assert file_info.path == Path("/root/test.txt")
        new_info = file_info.replace(name="new_name")
        assert new_info.name == "new_name"
        assert new_info.ext == file_info.ext
        assert new_info.path == file_info.path

    def test_from_dict(self):
        # This dict does not have enough params
        with pytest.raises(MissingValueError):
            dict_info = {"name": "dict_test"}
            FileInfo.from_dict(data=dict_info)
        # This does
        dict_info = {"name": "dict_test.txt", "ext": ".txt", "path": "/root"}
        file_info = FileInfo.from_dict(dict_info)
        assert file_info.name == dict_info["name"]
        assert file_info.ext == dict_info["ext"]
        assert file_info.path == Path(dict_info["path"])


class TestJSONEncode:
    def test_with_valid_input(self):
        data = json.dumps(123, cls=DataJSONEncoder)
        assert json.loads(data) == 123

    def test_with_invalid_input(self, make_json_fail):
        with pytest.raises(TypeError):
            json.dumps(make_json_fail, cls=DataJSONEncoder)
