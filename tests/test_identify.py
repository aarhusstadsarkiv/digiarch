from pathlib import Path
from uuid import UUID

from acacore.database import FilesDB
from acacore.models.file import OriginalFile
from acacore.models.reference_files import ActionData

from digiarch.cli import app
from digiarch.common import AVID
from tests.conftest import run_click


# noinspection DuplicatedCode
def test_identify_original(reference_files: Path, avid_folder: Path, avid_folder_copy: Path):
    avid = AVID(avid_folder)
    avid_copy = AVID(avid_folder_copy)
    avid_copy.database_path.unlink(missing_ok=True)

    run_click(avid_copy.path, app, "init", avid_copy.path)
    run_click(avid_copy.path, app, "identify", "original", "--siegfried-home", reference_files)

    with (
        FilesDB(avid.database_path) as base_db,
        FilesDB(avid_copy.database_path) as test_db,
    ):
        base_files = {str(f.relative_path): f for f in base_db.original_files}
        test_files = {str(f.relative_path): f for f in test_db.original_files}
        for path, base_file in base_files.items():
            test_file = test_files.get(path)
            assert test_file is not None
            assert base_file.checksum == test_file.checksum
            assert base_file.size == test_file.size
            assert base_file.puid == test_file.puid
            assert base_file.action == test_file.action
            assert base_file.action_data.model_dump(mode="json") == test_file.action_data.model_dump(mode="json")
            assert base_file.processed == test_file.processed
            assert base_file.original_path == test_file.original_path


# noinspection DuplicatedCode
def test_identify_master(reference_files: Path, avid_folder: Path, avid_folder_copy: Path):
    avid = AVID(avid_folder)
    avid_copy = AVID(avid_folder_copy)

    run_click(avid_copy.path, app, "identify", "master", "--siegfried-home", reference_files)

    with (
        FilesDB(avid.database_path) as base_db,
        FilesDB(avid_copy.database_path) as test_db,
    ):
        base_files = {str(f.relative_path): f for f in base_db.master_files}
        test_files = {str(f.relative_path): f for f in test_db.master_files}
        for path, base_file in base_files.items():
            test_file = test_files.get(path)
            assert test_file is not None
            assert base_file.checksum == test_file.checksum
            assert base_file.size == test_file.size
            assert base_file.puid == test_file.puid
            assert base_file.action == test_file.action
            assert base_file.action_data.model_dump(mode="json") == test_file.action_data.model_dump(mode="json")
            assert base_file.processed == test_file.processed
            assert base_file.original_uuid == test_file.original_uuid


def test_reidentify_original(reference_files: Path, avid_folder: Path, avid_folder_copy: Path):
    avid = AVID(avid_folder)
    avid_copy = AVID(avid_folder_copy)

    with FilesDB(avid_copy.database_path) as test_db:
        files: list[OriginalFile] = test_db.original_files.select("warning is not null and puid is not null").fetchall()
        uuids: list[UUID] = [f.uuid for f in files]
        for f in files:
            f.puid = f.signature = f.warning = f.action = None
            f.action_data = ActionData()
            test_db.original_files.update(f)
            test_db.commit()
            assert test_db.original_files[f].puid is None

    run_click(
        avid_copy.path,
        app,
        "identify",
        "original",
        f"@uuid {' '.join(map(str, (f.uuid for f in files)))}",
        "--siegfried-home",
        reference_files,
    )

    with (
        FilesDB(avid.database_path) as base_db,
        FilesDB(avid_copy.database_path) as test_db,
    ):
        base_files = {str(f.relative_path): f for f in base_db.original_files if f.uuid in uuids}
        test_files = {str(f.relative_path): f for f in test_db.original_files if f.uuid in uuids}
        for path, base_file in base_files.items():
            test_file = test_files.get(path)
            assert test_file is not None
            assert base_file.puid == test_file.puid
            assert base_file.action == test_file.action
            assert base_file.action_data.model_dump(mode="json") == test_file.action_data.model_dump(mode="json")
