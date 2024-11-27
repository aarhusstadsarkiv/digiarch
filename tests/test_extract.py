from pathlib import Path

from acacore.database import FilesDB
from acacore.models.file import OriginalFile

from digiarch.cli import app
from digiarch.common import AVID
from tests.conftest import run_click


def test_extract(avid_folder_copy: Path):
    avid = AVID(avid_folder_copy)

    with FilesDB(avid.database_path) as database:
        base_files: list[OriginalFile] = database.original_files.select("action = 'extract'").fetchall()

    run_click(avid_folder_copy, app, "extract")

    with FilesDB(avid.database_path) as database:
        for base_file in base_files:
            test_file = database.original_files[base_file]
            assert test_file

            if base_file.relative_path.name.split(".", 1)[0].endswith("-encrypted"):
                assert test_file.action == "ignore"
                assert test_file.action_data.ignore.template == "password-protected"
                continue

            if on_success_action := base_file.action_data.extract.on_success:
                assert test_file.action == on_success_action
                continue

            assert test_file.action == "ignore"
            assert test_file.action_data.ignore.template == "extracted-archive"

            extracted_files = database.original_files.select("parent = ?", [str(test_file.uuid)]).fetchall()
            assert extracted_files, repr(test_file)

            for child_file in extracted_files:
                assert child_file.puid
                assert child_file.action
                assert child_file.relative_path.relative_to(test_file.relative_path.parent)
