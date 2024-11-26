from pathlib import Path

import pytest
from acacore.__version__ import __version__ as acacore_version
from acacore.database import FilesDB
from click import UsageError

from digiarch.__version__ import __version__ as digiarch_version
from digiarch.cli import app
from digiarch.common import AVID

from .conftest import run_click


def test_init(avid_folder_copy: Path):
    avid = AVID(avid_folder_copy)
    avid.database_path.unlink(missing_ok=True)

    run_click(avid_folder_copy, app, "init", avid.path)

    assert avid.database_path.is_file()

    with FilesDB(avid.database_path, check_initialisation=True, check_version=True) as db:
        events = db.log.select(order_by=[("time", "asc")]).fetchall()
        assert len(events) == 3
        assert events[0].operation == f"{app.name}.init:start"
        assert isinstance(events[0].data, dict)
        # noinspection PyUnresolvedReferences
        assert events[0].data["version"] == digiarch_version
        # noinspection PyUnresolvedReferences
        assert events[0].data["acacore"] == acacore_version
        # noinspection PyUnresolvedReferences
        assert events[0].data["params"]["avid"] == str(avid.path)
        assert events[1].operation == f"{app.name}.init:initialized"
        assert events[1].data == acacore_version
        assert events[2].operation == f"{app.name}.init:end"
        assert events[2].data is None
        assert events[2].reason is None


def test_init_invalid(avid_folder_copy: Path):
    avid = AVID(avid_folder_copy)

    with pytest.raises(UsageError, match=f"{str(avid.metadata_dir)!r} is not a valid AVID directory."):
        run_click(avid.metadata_dir, app, "init", avid.metadata_dir)
