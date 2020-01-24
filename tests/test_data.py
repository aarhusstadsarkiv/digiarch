import json
import pytest
from pathlib import Path
from datetime import datetime
from digiarch.data import (
    FileInfo,
    Metadata,
    FileData,
    DataJSONEncoder,
    size_fmt,
)
from dacite import MissingValueError


@pytest.fixture
def file_info(temp_dir):
    test_file: Path = Path(temp_dir, "test.txt")
    test_file.touch(exist_ok=True)
    return FileInfo(path=test_file)


class FailJSON:
    def func(self):
        pass


@pytest.fixture
def make_json_fail():
    return FailJSON


class TestFileInfo:
    def test_init(self, temp_dir):
        # We cannot create an empty FileInfo.
        with pytest.raises(TypeError):
            FileInfo()  # type: ignore
        test_file: Path = Path(temp_dir, "test.txt")
        test_file.touch()
        f_info = FileInfo(path=test_file)
        assert f_info.name == "test.txt"
        assert f_info.ext == ".txt"
        assert f_info.path == Path(temp_dir, "test.txt")
        assert f_info.checksum is None
        assert f_info.identification is None

    def test_to_dict(self, file_info, temp_dir):
        dict_info = file_info.to_dict()
        assert dict_info["name"] == "test.txt"
        assert dict_info["ext"] == ".txt"
        assert dict_info["path"] == Path(temp_dir, "test.txt")

    def test_replace(self, file_info, temp_dir):
        new_test = Path(temp_dir, "new_test.json")
        new_test.touch()
        new_info = file_info.replace(path=new_test)
        assert new_info.name == "new_test.json"
        assert new_info.ext == ".json"

    def test_from_dict(self, temp_dir):
        # This dict does not have correct params
        with pytest.raises(MissingValueError):
            dict_info = {"name": "dict_test"}
            FileInfo.from_dict(data=dict_info)
        # This does
        test_file: Path = Path(temp_dir, "test.txt")
        test_file.touch(exist_ok=True)
        dict_info = {"path": str(test_file)}
        file_info = FileInfo.from_dict(dict_info)
        assert file_info.path == Path(dict_info["path"])


class TestMetadata:
    def test_init(self):
        # We cannot create an empty Metadata.
        with pytest.raises(TypeError):
            Metadata()  # type: ignore
        cur_time = datetime.now()
        metadata = Metadata(last_run=cur_time, processed_dir=Path("/"))
        assert metadata.last_run == cur_time
        assert metadata.processed_dir == Path("/")
        assert metadata.file_count is None
        assert metadata.total_size is None
        assert metadata.duplicates is None
        assert metadata.identification_warnings is None
        assert metadata.empty_subdirs is None
        assert metadata.several_files is None

    def test_post_init(self):
        cur_time = datetime.now()
        cur_time_str = cur_time.isoformat()
        metadata = Metadata(last_run=cur_time_str, processed_dir="/")
        assert metadata.last_run == cur_time
        assert metadata.processed_dir == Path("/")


class TestFileData:
    def test_init(self, temp_dir):
        # We cannot create an empty FileData.
        with pytest.raises(TypeError):
            FileData()  # type: ignore
        cur_time = datetime.now()
        metadata = Metadata(last_run=cur_time, processed_dir=Path(temp_dir))
        file_data = FileData(metadata)
        assert file_data.metadata == metadata
        assert file_data.files == []

    def test_post_init(self, temp_dir):
        cur_time = datetime.now()
        metadata = Metadata(last_run=cur_time, processed_dir=str(temp_dir))
        file_data = FileData(metadata)
        assert file_data.digiarch_dir == Path(
            metadata.processed_dir, "_digiarch"
        )
        assert (
            file_data.json_file
            == file_data.digiarch_dir / ".data" / "data.json"
        )

    def test_functions(self, temp_dir):
        cur_time = datetime.now()
        metadata = Metadata(last_run=cur_time, processed_dir=str(temp_dir))
        file_data = FileData(metadata)
        file_data.to_json()
        new_file_data = FileData.from_json(file_data.json_file)
        assert new_file_data == file_data


class TestJSONEncode:
    def test_with_valid_input(self):
        data = json.dumps(123, cls=DataJSONEncoder)
        assert json.loads(data) == 123

    def test_with_invalid_input(self, make_json_fail):
        with pytest.raises(TypeError):
            json.dumps(make_json_fail, cls=DataJSONEncoder)


class TestAuxFunctions:
    def test_size_fmt(self):
        assert size_fmt(2 ** 0) == "1.0 B"
        assert size_fmt(2 ** 10) == "1.0 KiB"
        assert size_fmt(2 ** 20) == "1.0 MiB"
        assert size_fmt(2 ** 30) == "1.0 GiB"
        assert size_fmt(2 ** 40) == "1.0 TiB"
