from json import dumps
from pathlib import Path
from shutil import copy

import pytest
from acacore.models.file import File
from acacore.models.history import HistoryEntry
from acacore.models.reference_files import ActionData
from acacore.models.reference_files import ConvertAction
from acacore.models.reference_files import ExtractAction
from acacore.models.reference_files import IgnoreAction
from acacore.models.reference_files import ManualAction
from acacore.models.reference_files import ReIdentifyAction
from acacore.models.reference_files import RenameAction
from acacore.models.reference_files import ReplaceAction
from acacore.utils.functions import rm_tree

from digiarch.cli import app
from digiarch.cli import app_edit
from digiarch.cli import app_edit_action
from digiarch.cli import app_identify
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

    rm_tree(new_files_folder)

    new_files_folder.mkdir(parents=True, exist_ok=True)

    for file in files_folder.iterdir():
        if file.is_file():
            copy(file, new_files_folder / file.name)

    return new_files_folder


def test_identify(tests_folder: Path, files_folder: Path, files_folder_copy: Path):
    args: list[str] = [
        app_identify.name,
        str(files_folder_copy),
        "--actions",
        str(tests_folder / "fileformats.yml"),
        "--custom-signatures",
        str(tests_folder / "custom_signatures.json"),
        "--no-update-siegfried-signature",
        "--siegfried-home",
        str(tests_folder),
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

    rm_tree(files_folder_copy / "_metadata")

    app.main([*args, "--siegfried-path", str(tests_folder / "sf")], standalone_mode=False)

    with FileDB(files_folder_copy / "_metadata" / "files.db") as database:
        last_history: HistoryEntry = sorted(database.history, key=lambda h: h.time).pop()
        assert isinstance(last_history.data, str)
        assert last_history.data.startswith("FileNotFoundError")
        assert last_history.reason is not None


def test_edit_action(tests_folder: Path, files_folder: Path, files_folder_copy: Path):
    database_path: Path = files_folder / "_metadata" / "files.db"
    database_path_copy: Path = files_folder_copy / database_path.relative_to(files_folder)
    database_path_copy.parent.mkdir(parents=True, exist_ok=True)
    copy(database_path, database_path_copy)

    with FileDB(database_path_copy) as database:
        file: File = database.files.select(limit=1, order_by=[("random()", "asc")]).fetchone()
        assert isinstance(file, File)

    file.action_data = ActionData()

    for action in ("convert", "extract", "replace", "manual", "rename", "ignore", "reidentify"):
        previous_action = file.action
        file.action = action

        if action == "convert":
            file.action_data.convert = [ConvertAction(converter="test", converter_type="master", outputs=["ext"])]
        elif action == "extract":
            file.action_data.extract = ExtractAction(tool="tool", dir_suffix="dir_suffix")
        elif action == "replace":
            file.action_data.replace = ReplaceAction(template="empty")
        elif action == "manual":
            file.action_data.manual = ManualAction(reason="reason", process="process")
        elif action == "rename":
            file.action_data.rename = RenameAction(extension="ext")
        elif action == "ignore":
            file.action_data.ignore = IgnoreAction(reason="reason")
        elif action == "reidentify":
            file.action_data.reidentify = ReIdentifyAction(reason="reason")

        args: list[str] = [
            app_edit.name,
            app_edit_action.name,
            str(files_folder_copy),
            str(file.uuid),
            action,
            f"edit action {action}",
            "--data-json",
            dumps(file.action_data.model_dump(mode="json")[action]),
        ]

        app.main(args, standalone_mode=False)

        with FileDB(database_path_copy) as database:
            file2: File = database.files.select(where="UUID = ?", limit=1, parameters=[str(file.uuid)]).fetchone()
            assert file2.action_data.model_dump()[action] == file.action_data.model_dump()[action]

            history: HistoryEntry = database.history.select(
                where="UUID = ? and OPERATION like '%:file:edit:action'",
                order_by=[("TIME", "desc")],
                limit=1,
                parameters=[str(file.uuid)],
            ).fetchone()

            assert history.data == [previous_action, action]
            assert history.reason == f"edit action {action}"
