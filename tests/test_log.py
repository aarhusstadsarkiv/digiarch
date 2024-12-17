from pathlib import Path

from digiarch.cli import app
from digiarch.common import AVID
from tests.conftest import run_click


def test_log(avid_folder_copy: Path):
    avid = AVID(avid_folder_copy)

    run_click(avid.path, app, "log", "--order", "desc")
    run_click(avid.path, app, "log", "--run-only")
