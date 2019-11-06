import json
import pytest
from digiarch.data import FileInfo, encode_dataclass


@pytest.fixture
def file_info():
    return FileInfo(name="test.txt", ext=".txt", path="/root")


class FailJSON:
    def func(self):
        pass


@pytest.fixture
def make_json_fail():
    return FailJSON


class TestFileInfo:
    def test_to_json(self, file_info):
        file_info_json = file_info.to_json()
        assert json.loads(file_info_json) == {
            "name": "test.txt",
            "ext": ".txt",
            "is_empty_sub": False,
            "path": "/root",
            "mime_type": "",
            "guessed_ext": "",
        }


class TestJSONEncode:
    def test_with_valid_input(self):
        data = json.dumps(123, default=encode_dataclass)
        assert json.loads(data) == 123

    def test_with_invalid_input(self, make_json_fail):
        with pytest.raises(TypeError):
            assert json.dumps(make_json_fail, default=encode_dataclass)
