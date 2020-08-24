# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------

import json
from pathlib import Path

import pytest
from digiarch.identify.reports import report_results
from digiarch.internals import ArchiveFile

# -----------------------------------------------------------------------------
# Fixtures
# -----------------------------------------------------------------------------


@pytest.fixture
def file_info_0(temp_dir):
    test_file = temp_dir / "test0.txt"
    test_file.write_text("0")
    file_info = ArchiveFile(path=test_file)
    return file_info


@pytest.fixture
def file_info_1(temp_dir):
    test_file = temp_dir / "test1.pdf"
    test_file.write_text("1")
    file_info = ArchiveFile(
        path=test_file, puid=None, signature=None, warning="No match"
    )
    return file_info


@pytest.fixture
def file_info_2(temp_dir):
    test_file = temp_dir / "test3.bogus"
    test_file.write_text("1")
    file_info = ArchiveFile(
        path=test_file, puid=None, signature=None, warning="Extension mismatch"
    )
    return file_info


# -----------------------------------------------------------------------------
# Tests
# -----------------------------------------------------------------------------


class TestReportResults:
    def test_with_file(self, file_info_0, temp_dir):
        file_list = [file_info_0]
        report_results(file_list, temp_dir)
        file_exts = json.load(
            Path(temp_dir / "reports" / "file_extensions.json").open(
                encoding="utf-8"
            )
        )
        id_warnings = json.load(
            Path(temp_dir / "reports" / "identification_warnings.json").open(
                encoding="utf-8"
            )
        )
        assert ".txt" in file_exts.keys()
        assert sum(id_warnings["counts"].values()) == 0
        assert not id_warnings["warnings"]

    def test_with_id_warnings(self, file_info_1, file_info_2, temp_dir):
        file_list = [file_info_1, file_info_2]
        report_results(file_list, temp_dir)
        file_exts = json.load(
            Path(temp_dir / "reports" / "file_extensions.json").open(
                encoding="utf-8"
            )
        )
        id_warnings = json.load(
            Path(temp_dir / "reports" / "identification_warnings.json").open(
                encoding="utf-8"
            )
        )
        assert ".bogus" in file_exts.keys()
        assert ".pdf" in file_exts.keys()
        assert sum(id_warnings["counts"].values()) == 2
        assert "No match" in id_warnings["warnings"].keys()
        assert "Extension mismatch" in id_warnings["warnings"].keys()

    def test_without_files(self, temp_dir):
        report_results([], temp_dir)
        contents = [str(p) for p in Path(temp_dir / "reports").rglob("*")]
        assert not any(
            "file_extensions.json" in content for content in contents
        )
        assert not any(
            "identification_warnings.json" in content for content in contents
        )
