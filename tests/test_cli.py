from json import dumps
from pathlib import Path
from shutil import copy
from typing import Optional

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
from digiarch.cli import app_edit_remove
from digiarch.cli import app_edit_rename
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
    new_files_folder.mkdir(parents=True, exist_ok=True)

    for file in files_folder.iterdir():
        if file.is_file():
            copy(file, new_files_folder / file.name)

    return new_files_folder


def test_identify(tests_folder: Path, files_folder: Path, files_folder_copy: Path):
    database_path: Path = files_folder / "_metadata" / "files.db"
    database_path_copy: Path = files_folder_copy / database_path.relative_to(files_folder)

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
        FileDB(database_path) as baseline,
        FileDB(database_path_copy) as database,
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

    database_path_copy.unlink(missing_ok=True)

    app.main([*args, "--siegfried-path", str(tests_folder / "sf")], standalone_mode=False)

    with FileDB(database_path_copy) as database:
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
        file: File = database.files.select(where="puid is not null", limit=1, order_by=[("random()", "asc")]).fetchone()
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

        args: list[str] = [
            app_edit.name,
            app_edit_action.name,
            str(files_folder_copy),
            str(file.puid),
            "replace",
            "edit action with puid",
            "--data",
            "template",
            "empty",
            "--puid",
        ]

        app.main(args, standalone_mode=False)

        with FileDB(database_path_copy) as database:
            files: list[File] = list(database.files.select(where="PUID = ?", limit=1, parameters=[file.puid]))

            assert all(f.action == "replace" for f in files)
            assert all(f.action_data and f.action_data.replace for f in files)
            assert all(f.action_data.replace.template == "empty" for f in files)

            for file in files:
                history: HistoryEntry = database.history.select(
                    where="UUID = ? and OPERATION like '%:file:edit:action'",
                    order_by=[("TIME", "desc")],
                    limit=1,
                    parameters=[str(file.uuid)],
                ).fetchone()

                assert isinstance(history.data, list)
                assert history.data[-1] == "replace"
                assert history.reason == "edit action with puid"


def test_edit_action_ids_file(tests_folder: Path, files_folder: Path, files_folder_copy: Path):
    database_path: Path = files_folder / "_metadata" / "files.db"
    database_path_copy: Path = files_folder_copy / database_path.relative_to(files_folder)
    database_path_copy.parent.mkdir(parents=True, exist_ok=True)
    copy(database_path, database_path_copy)

    with FileDB(database_path_copy) as database:
        files: list[File] = list(database.files.select(order_by=[("random()", "asc")], limit=3))

    ids_file: Path = files_folder_copy.joinpath("ids.txt")
    ids_file.write_text("\n".join(str(f.uuid) for f in files))
    test_action: str = "ignore"
    test_reason: str = "edit action with ids file"

    args: list[str] = [
        app_edit.name,
        app_edit_action.name,
        "--uuid",
        "--id-files",
        str(files_folder_copy),
        str(ids_file),
        test_action,
        test_reason,
        "--data",
        "reason",
        test_reason,
    ]

    app.main(args, standalone_mode=False)

    with FileDB(database_path_copy) as database:
        for file in files:
            file_new: Optional[File] = database.files.select(
                where="uuid = ?",
                parameters=[str(file.uuid)],
                limit=1,
            ).fetchone()
            assert file_new is not None
            assert file_new.action == "ignore"
            assert file_new.action_data.ignore
            assert file_new.action_data.ignore.reason == test_reason

            history_edit: Optional[HistoryEntry] = database.history.select(
                where="uuid = ? and operation like ? || '%'",
                parameters=[str(file.uuid), "digiarch.edit.action:"],
            ).fetchone()
            assert history_edit is not None
            assert history_edit.reason == test_reason


def test_edit_remove(files_folder: Path, files_folder_copy: Path):
    database_path: Path = files_folder / "_metadata" / "files.db"
    database_path_copy: Path = files_folder_copy / database_path.relative_to(files_folder)
    database_path_copy.parent.mkdir(parents=True, exist_ok=True)
    copy(database_path, database_path_copy)

    with FileDB(database_path_copy) as database:
        file: File = database.files.select(limit=1, order_by=[("random()", "asc")]).fetchone()
        assert isinstance(file, File)

    args: list[str] = [
        app_edit.name,
        app_edit_remove.name,
        "--uuid",
        str(files_folder_copy),
        str(file.uuid),
        "Remove file with uuid",
    ]

    app.main(args, standalone_mode=False)

    with FileDB(database_path_copy) as database:
        file: Optional[File] = database.files.select(where="uuid = ?", limit=1, parameters=[str(file.uuid)]).fetchone()
        assert file is None


def test_edit_remove_ids_file(tests_folder: Path, files_folder: Path, files_folder_copy: Path):
    database_path: Path = files_folder / "_metadata" / "files.db"
    database_path_copy: Path = files_folder_copy / database_path.relative_to(files_folder)
    database_path_copy.parent.mkdir(parents=True, exist_ok=True)
    copy(database_path, database_path_copy)

    with FileDB(database_path_copy) as database:
        files: list[File] = list(database.files.select(order_by=[("random()", "asc")], limit=3))

    ids_file: Path = files_folder_copy.joinpath("ids.txt")
    ids_file.write_text("\n".join(str(f.uuid) for f in files))
    test_reason: str = "edit action with ids file"

    args: list[str] = [
        app_edit.name,
        app_edit_remove.name,
        "--uuid",
        "--id-files",
        str(files_folder_copy),
        str(ids_file),
        test_reason,
    ]

    app.main(args, standalone_mode=False)

    with FileDB(database_path_copy) as database:
        for file in files:
            file_new: Optional[File] = database.files.select(where="uuid = ?", parameters=[str(file.uuid)]).fetchone()
            assert file_new is None

            history_edit: Optional[HistoryEntry] = database.history.select(
                where="uuid = ? and operation like ? || '%'",
                parameters=[str(file.uuid), "digiarch.edit.remove:"],
            ).fetchone()
            assert history_edit is not None
            assert history_edit.reason == test_reason


def test_edit_rename(files_folder: Path, files_folder_copy: Path):
    database_path: Path = files_folder / "_metadata" / "files.db"
    database_path_copy: Path = files_folder_copy / database_path.relative_to(files_folder)
    database_path_copy.parent.mkdir(parents=True, exist_ok=True)
    copy(database_path, database_path_copy)

    with FileDB(database_path_copy) as database:
        file_old: File = database.files.select(limit=1, order_by=[("random()", "asc")]).fetchone()
        assert isinstance(file_old, File)
        file_old.root = files_folder_copy

    test_extension: str = ".test"
    test_reason: str = "edit extension"

    args: list[str] = [
        app_edit.name,
        app_edit_rename.name,
        "--uuid",
        str(files_folder_copy),
        str(file_old.uuid),
        "{suffixes}" + test_extension,
        test_reason,
    ]

    app.main(args, standalone_mode=False)

    with FileDB(database_path_copy) as database:
        file_new: Optional[File] = database.files.select(where="uuid = ?", parameters=[str(file_old.uuid)]).fetchone()
        assert isinstance(file_old, File)
        file_new.root = files_folder_copy
        assert file_new.name == file_old.name + test_extension
        assert file_new.get_absolute_path().is_file()
        assert not file_old.get_absolute_path().is_file()

        history_edit: Optional[HistoryEntry] = database.history.select(
            where="uuid = ? and operation like ? || '%'",
            parameters=[str(file_old.uuid), "digiarch.edit.rename:"],
        ).fetchone()
        assert history_edit is not None
        assert history_edit.reason == test_reason


def test_edit_rename_same(files_folder: Path, files_folder_copy: Path):
    database_path: Path = files_folder / "_metadata" / "files.db"
    database_path_copy: Path = files_folder_copy / database_path.relative_to(files_folder)
    database_path_copy.parent.mkdir(parents=True, exist_ok=True)
    copy(database_path, database_path_copy)

    with FileDB(database_path_copy) as database:
        file_old: File = database.files.select(limit=1, order_by=[("random()", "asc")]).fetchone()
        assert isinstance(file_old, File)
        file_old.root = files_folder_copy

    test_extension: str = "{suffixes}"
    test_reason: str = "edit extension same"

    args: list[str] = [
        app_edit.name,
        app_edit_rename.name,
        "--uuid",
        str(files_folder_copy),
        str(file_old.uuid),
        test_extension,
        test_reason,
    ]

    app.main(args, standalone_mode=False)

    with FileDB(database_path_copy) as database:
        file_new: Optional[File] = database.files.select(where="uuid = ?", parameters=[str(file_old.uuid)]).fetchone()
        assert isinstance(file_new, File)
        file_new.root = files_folder_copy
        assert file_new.name == file_old.name
        assert file_new.get_absolute_path().is_file()
        assert file_old.get_absolute_path().is_file()
