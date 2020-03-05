import pytest
import yaml
from subprocess import CalledProcessError
from pathlib import Path
from unittest.mock import patch
from digiarch.internals import FileInfo, Identification
from digiarch.identify.identify_files import sf_id
from digiarch.exceptions import IdentificationError


@pytest.fixture
def adx_info(test_data_dir):
    adx_file: Path = test_data_dir / "adx_test.adx"
    return FileInfo(path=adx_file)


@pytest.fixture
def docx_info(test_data_dir):
    docx_file: Path = test_data_dir / "docx_test.docx"
    return FileInfo(path=docx_file)


class TestSFId:
    def test_valid_input(self, docx_info):
        result = sf_id(docx_info).identification or Identification(None, None)
        assert result.puid == "fmt/412"
        assert result.signame == "Microsoft Word for Windows"
        assert result.warning is None

    def test_unknown_puid(self, adx_info):
        result = sf_id(adx_info).identification or Identification(None, None)
        assert result.puid is None
        assert "No match" in (result.warning or "")

    def test_subprocess_error(self, docx_info):
        with pytest.raises(IdentificationError):
            with patch(
                "subprocess.run",
                side_effect=CalledProcessError(
                    returncode=1, cmd="Fail", stderr=b"Fail"
                ),
            ):
                sf_id(docx_info)

    def test_yaml_error(self, docx_info):
        with pytest.raises(IdentificationError):
            with patch(
                "yaml.safe_load_all", side_effect=yaml.YAMLError,
            ):
                sf_id(docx_info)
