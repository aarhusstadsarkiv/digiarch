from pathlib import Path

from digiarch.cli import app
from digiarch.common import AVID

from tests.conftest import run_click


def test_search(avid_folder_copy: Path):
    avid = AVID(avid_folder_copy)

    run_click(avid.path, app, "search", "original", "@relative_path %")
    run_click(avid.path, app, "search", "master", "@relative_path %")
    run_click(avid.path, app, "search", "access", "@relative_path %")
    run_click(avid.path, app, "search", "statutory", "@relative_path %")
