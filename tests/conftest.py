"""Shared testing fixtures.

"""

# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------
import pytest
import os

# -----------------------------------------------------------------------------
# Function Definitions
# -----------------------------------------------------------------------------


@pytest.fixture
def temp_dir(tmpdir_factory):
    temp_dir: str = tmpdir_factory.mktemp("temp_dir")
    return temp_dir


@pytest.fixture
def main_dir(temp_dir):
    main_dir: str = temp_dir.mkdir("_digiarch")
    return main_dir


@pytest.fixture
def data_file(main_dir):
    data_dir: str = main_dir.mkdir(".data")
    data_file: str = os.path.join(data_dir, "data.json")
    return data_file
