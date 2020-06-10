"""Shared testing fixtures.

"""

# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------

import inspect
from datetime import datetime
from pathlib import Path

import pytest

from digiarch.internals import FileData, Metadata

# -----------------------------------------------------------------------------
# Function Definitions
# -----------------------------------------------------------------------------


@pytest.fixture
def temp_dir(tmpdir_factory):
    temp_dir: str = tmpdir_factory.mktemp("temp_dir")
    return Path(temp_dir)


@pytest.fixture
def main_dir(temp_dir):
    main_dir: Path = temp_dir / "_digiarch"
    main_dir.mkdir(exist_ok=True)
    return main_dir


@pytest.fixture
def data_file(main_dir):
    data_dir: Path = main_dir / ".data"
    data_dir.mkdir(exist_ok=True)
    data_file: Path = data_dir / "data.json"
    return data_file


@pytest.fixture
def file_data(temp_dir):
    cur_time = datetime.now()
    return FileData(Metadata(cur_time, Path(temp_dir)))


@pytest.fixture
def test_data_dir():
    return Path(inspect.getfile(FileData)).parent.parent / "tests" / "_data"
