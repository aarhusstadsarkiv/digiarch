from json import dumps
from pathlib import Path
from shutil import copy

import pytest
from acacore.models.history import HistoryEntry
from acacore.utils.functions import rm_tree

from digiarch.cli import app
from digiarch.cli import app_process
from digiarch.database import FileDB


@pytest.fixture()
def tests_folder() -> Path:
    return Path(__file__).parent


@pytest.fixture()
def files_folder(tests_folder: Path) -> Path:
    return tests_folder / "files"


@pytest.fixture()
def files_folder_copy(files_folder: Path, tests_folder: Path) -> Path:
    new_files_folder: Path = tests_folder / f"_{files_folder.name}"

    if new_files_folder.is_dir():
        rm_tree(new_files_folder)

    new_files_folder.mkdir(parents=True, exist_ok=True)

    for file in files_folder.iterdir():
        if file.is_file():
            copy(file, new_files_folder / file.name)

    return new_files_folder


def test_identify(tests_folder: Path, files_folder: Path, files_folder_copy: Path):
    args: list[str] = [
        app_process.name,
        str(files_folder_copy),
        "--actions",
        str(tests_folder / "fileformats.yml"),
        "--custom-signatures",
        str(tests_folder / "custom_signatures.json"),
        "--no-update-siegfried-signature",
    ]

    app.main(args, standalone_mode=False)

    with (
        FileDB(files_folder / "_metadata" / "files.db") as baseline,
        FileDB(files_folder_copy / "_metadata" / "files.db") as database,
    ):
        baseline_files = {
            (
                f.checksum,
                f.relative_path,
                f.size,
                f.puid,
                f.action,
                f.action_data.model_dump_json() if f.action_data else None,
                f.processed,
            )
            for f in baseline.files
        }
        database_files = {
            (
                f.checksum,
                f.relative_path,
                f.size,
                f.puid,
                f.action,
                f.action_data.model_dump_json() if f.action_data else None,
                f.processed,
            )
            for f in database.files
        }
        assert baseline_files == database_files

        baseline_history = {(h.operation, dumps(h.data), dumps(h.reason)) for h in baseline.history if h.uuid}
        database_history = {(h.operation, dumps(h.data), dumps(h.reason)) for h in database.history if h.uuid}
        assert baseline_history == database_history

    rm_tree(files_folder_copy / "_metadata")

    app.main([*args, "--siegfried-path", str(tests_folder / "sf")], standalone_mode=False)

    with FileDB(files_folder_copy / "_metadata" / "files.db") as database:
        last_history: HistoryEntry = sorted(database.history, key=lambda h: h.time).pop()
        assert last_history.data == 1
        assert last_history.reason is not None
