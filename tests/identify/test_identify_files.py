# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------

import json
from pathlib import Path
from subprocess import CalledProcessError
from unittest.mock import patch

import pytest

from digiarch.exceptions import IdentificationError
from digiarch.internals import Identification
from digiarch.identify.identify_files import sf_id, custom_id

# -----------------------------------------------------------------------------
# Fixtures
# -----------------------------------------------------------------------------


@pytest.fixture
def adx_info(test_data_dir):
    adx_file: Path = test_data_dir / "adx_test.adx"
    return adx_file


@pytest.fixture
def docx_info(test_data_dir):
    docx_file: Path = test_data_dir / "docx_test.docx"
    return docx_file


# -----------------------------------------------------------------------------
# Tests
# -----------------------------------------------------------------------------


class TestSFId:
    def test_valid_input(self, docx_info):
        result = sf_id(docx_info)
        paths = result.keys()
        puids = [identification.puid for identification in result.values()]
        signames = [
            identification.signame for identification in result.values()
        ]
        warnings = [
            identification.warning for identification in result.values()
        ]
        assert docx_info in paths
        assert "fmt/412" in puids
        assert "Microsoft Word for Windows" in signames
        assert not any(warnings)

    def test_unknown_puid(self, adx_info):
        result = sf_id(adx_info)
        paths = result.keys()
        puids = [identification.puid for identification in result.values()]
        signames = [
            identification.signame for identification in result.values()
        ]
        warnings = [
            identification.warning for identification in result.values()
        ]
        assert adx_info in paths
        assert not any(puids)
        assert not any(signames)
        assert (
            "No match; possibilities based on extension are fmt/840"
            in warnings
        )

    def test_subprocess_error(self, docx_info):
        with pytest.raises(IdentificationError):
            with patch(
                "subprocess.run",
                side_effect=CalledProcessError(
                    returncode=1, cmd="Fail", stderr=b"Fail"
                ),
            ):
                sf_id(docx_info)

    def test_json_error(self, docx_info):
        with pytest.raises(IdentificationError):
            with patch(
                "json.loads", side_effect=json.JSONDecodeError,
            ):
                sf_id(docx_info)


class TestCustomId:
    def test_lwp(self, temp_dir):
        lwp_file = temp_dir / "mock.lwp"
        lwp_file.write_bytes(
            bytes.fromhex(
                "576F726450726F0DFB000000000000"
                "000005985C8172030040CCC1BFFFBDF970"
            )
        )
        lwp_id = Identification(
            puid=None, signame=None, warning="this is a warning"
        )
        new_id = custom_id(lwp_file, lwp_id)
        assert new_id.puid == "x-fmt/340"
        assert new_id.signame == "Lotus WordPro Document"
        assert new_id.warning is None

    def test_123(self, temp_dir):
        _123_file = temp_dir / "mock.123"
        _123_file.write_bytes(bytes.fromhex("00001A000310040000000000"))
        _123_id = Identification(
            puid=None, signame=None, warning="this is a warning"
        )
        new_id = custom_id(_123_file, _123_id)
        assert new_id.puid == "aca-fmt/1"
        assert new_id.signame == "Lotus 1-2-3 Spreadsheet"
        assert new_id.warning is None
