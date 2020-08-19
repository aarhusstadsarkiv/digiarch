import json
from datetime import datetime
from pathlib import Path

from pydantic import ValidationError

import pytest
from digiarch.internals import DataJSONEncoder, FileData, Metadata, size_fmt


class FailJSON:
    def func(self):
        pass


@pytest.fixture
def make_json_fail():
    return FailJSON


class TestMetadata:
    def test_init(self, temp_dir):
        # We cannot create an empty Metadata.
        with pytest.raises(ValidationError):
            Metadata()  # type: ignore
        cur_time = datetime.now()
        metadata = Metadata(last_run=cur_time, processed_dir=Path(temp_dir))
        assert metadata.last_run == cur_time
        assert metadata.processed_dir == Path(temp_dir)
        assert metadata.file_count is None
        assert metadata.total_size is None
        assert metadata.duplicates is None
        assert metadata.identification_warnings is None
        assert metadata.empty_subdirs is None
        assert metadata.several_files is None

    def test_post_init(self, temp_dir):
        cur_time = datetime.now()
        metadata = Metadata(last_run=cur_time, processed_dir=Path(temp_dir))
        assert metadata.last_run == cur_time

        assert metadata.processed_dir == Path(temp_dir)


class TestFileData:
    def test_init(self, temp_dir):
        # We cannot create an empty FileData.
        with pytest.raises(ValidationError):
            FileData()  # type: ignore
        cur_time = datetime.now()
        metadata = Metadata(last_run=cur_time, processed_dir=Path(temp_dir))
        file_data = FileData(metadata=metadata)
        assert file_data.metadata == metadata
        assert file_data.files == []
        assert file_data.digiarch_dir == Path(
            metadata.processed_dir, "_digiarch"
        )
        assert (
            file_data.json_file
            == file_data.digiarch_dir / ".data" / "data.json"
        )

    def test_validators(self, temp_dir):
        cur_time = datetime.now()
        metadata = Metadata(last_run=cur_time, processed_dir=temp_dir)
        with pytest.raises(
            ValidationError, match="Invalid digiarch directory path"
        ):
            FileData(metadata=metadata, digiarch_dir=Path("fail"))
        with pytest.raises(ValidationError, match="Invalid JSON file path"):
            FileData(metadata=metadata, json_file=Path("fail"))

        temp_json = temp_dir / "test.json"
        temp_json.write_text("test")
        assert FileData(metadata=metadata, digiarch_dir=temp_dir)
        assert FileData(metadata=metadata, json_file=temp_json)


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
