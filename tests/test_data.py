import json
import pytest
from digiarch.data import FileInfo, DataJSONEncoder


@pytest.fixture
def file_info():
    return FileInfo(name="test.txt", ext=".txt", path="/root")


class FailJSON:
    def func(self):
        pass


@pytest.fixture
def make_json_fail():
    return FailJSON


class TestJSONEncode:
    def test_with_valid_input(self):
        data = json.dumps(123, cls=DataJSONEncoder)
        assert json.loads(data) == 123

    def test_with_invalid_input(self, make_json_fail):
        with pytest.raises(TypeError):
            assert json.dumps(make_json_fail, cls=DataJSONEncoder)
