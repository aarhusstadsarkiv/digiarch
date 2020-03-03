import pytest
from pathlib import Path
from digiarch.internals import FileInfo, Identification
from digiarch.identify.reports import report_results


@pytest.fixture
def file_info_0(temp_dir):
    test_file = Path(temp_dir).joinpath("test0.txt")
    test_file.write_text("0")
    file_info = FileInfo(test_file)
    return file_info


@pytest.fixture
def file_info_1(temp_dir):
    test_file = Path(temp_dir).joinpath("test1.pdf")
    test_file.write_text("1")
    file_info = FileInfo(
        test_file,
        identification=Identification(
            puid=None, signame=None, warning="No match"
        ),
    )
    return file_info


class TestReportResults:
    def test_with_file(self, file_info_0, temp_dir):
        file_list = [file_info_0]
        report_results(file_list, temp_dir)
        contents = [str(p) for p in Path(temp_dir / "reports").rglob("*")]
        assert any("file_extensions.json" in content for content in contents)
        assert not any(
            "identification_warnings.json" in content for content in contents
        )

    def test_with_id_warnings(self, file_info_1, temp_dir):
        file_list = [file_info_1]
        report_results(file_list, temp_dir)
        contents = [str(p) for p in Path(temp_dir / "reports").rglob("*")]
        assert any(
            "identification_warnings.json" in content for content in contents
        )

    def test_without_files(self, temp_dir):
        report_results([], temp_dir)
        contents = [str(p) for p in Path(temp_dir / "reports").rglob("*")]
        assert not any(
            "file_extensions.csv" in content for content in contents
        )
        assert not any(
            "identification_warnings.json" in content for content in contents
        )
