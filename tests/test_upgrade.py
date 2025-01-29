from pathlib import Path

from digiarch.cli import app
from digiarch.common import AVID

from tests.conftest import run_click


def test_upgrade(avid_folder_copy: Path):
    avid = AVID(avid_folder_copy)
    size: int = avid.database_path.stat().st_size
    m_time: float = avid.database_path.stat().st_mtime

    run_click(avid.path, app, "upgrade")

    assert avid.database_path.stat().st_size == size
    assert avid.database_path.stat().st_mtime == m_time
