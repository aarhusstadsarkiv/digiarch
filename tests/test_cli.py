from datetime import datetime
from pathlib import Path
from shutil import copy
from typing import Optional
from uuid import uuid4

import pytest
from acacore.database import FileDB
from acacore.models.file import File
from acacore.models.history import HistoryEntry
from acacore.models.reference_files import ConvertAction
from acacore.models.reference_files import ExtractAction
from acacore.models.reference_files import IgnoreAction
from acacore.models.reference_files import ManualAction
from acacore.utils.functions import find_files
from acacore.utils.functions import rm_tree
from click import BadParameter
from pydantic import BaseModel

from digiarch.cli import app
from digiarch.doctor import command_doctor
from digiarch.edit.action import group_action
from digiarch.edit.edit import group_edit
from digiarch.edit.lock import command_lock
from digiarch.edit.remove import command_remove
from digiarch.edit.rename import command_rename
from digiarch.edit.rollback import command_rollback
from digiarch.extract.extract import command_extract
from digiarch.history import command_history
from digiarch.identify import command_identify
from digiarch.identify import command_reidentify


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

    for file in find_files(files_folder, exclude=[files_folder / "_metadata"]):
        copy(file, new_files_folder / file.relative_to(files_folder))

    return new_files_folder


# noinspection DuplicatedCode
def test_identify(tests_folder: Path, files_folder: Path, files_folder_copy: Path):
    database_path: Path = files_folder / "_metadata" / "files.db"
    database_path_copy: Path = files_folder_copy / database_path.relative_to(files_folder)

    args: list[str] = [
        command_identify.name,
        str(files_folder_copy),
        "--actions",
        str(tests_folder / "fileformats.yml"),
        "--custom-signatures",
        str(tests_folder / "custom_signatures.yml"),
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


# noinspection DuplicatedCode
def test_reidentify(tests_folder: Path, files_folder: Path, files_folder_copy: Path):
    database_path: Path = files_folder / "_metadata" / "files.db"
    database_path_copy: Path = files_folder_copy / database_path.relative_to(files_folder)
    database_path_copy.parent.mkdir(parents=True, exist_ok=True)
    copy(database_path, database_path_copy)

    with FileDB(database_path_copy) as database:
        file: File = database.files.select(
            where="puid = ? and warning like '%' || ? || '%'",
            parameters=["fmt/11", '"extension mismatch"'],
            limit=1,
        ).fetchone()
        assert isinstance(file, File)
        file.root = files_folder_copy
        file.get_absolute_path().rename(file.get_absolute_path().with_suffix(".png"))
        file.relative_path = file.relative_path.with_suffix(".png")
        database.files.update(file, {"uuid": file.uuid})
        database.commit()

    app.main(
        [
            command_reidentify.name,
            str(files_folder_copy),
            "--uuid",
            str(file.uuid),
            "--actions",
            str(tests_folder / "fileformats.yml"),
            "--custom-signatures",
            str(tests_folder / "custom_signatures.yml"),
            "--siegfried-home",
            str(tests_folder),
        ],
        standalone_mode=False,
    )

    with FileDB(database_path_copy) as database:
        file_new: File = database.files.select(
            where="uuid = ? and relative_path = ?",
            parameters=[str(file.uuid), str(file.relative_path)],
            limit=1,
        ).fetchone()
        assert isinstance(file, File)
        assert "extension mismatch" not in (file_new.warning or [])


# noinspection DuplicatedCode
def test_extract(tests_folder: Path, files_folder: Path, files_folder_copy: Path):
    database_path: Path = files_folder / "_metadata" / "files.db"
    database_path_copy: Path = files_folder_copy / database_path.relative_to(files_folder)
    database_path_copy.parent.mkdir(parents=True, exist_ok=True)
    copy(database_path, database_path_copy)

    with FileDB(database_path_copy) as database:
        files: list[File] = list(database.files.select(where="action = 'extract'"))

    app.main(
        [
            command_extract.name,
            str(files_folder_copy),
            "--actions",
            str(tests_folder / "fileformats.yml"),
            "--custom-signatures",
            str(tests_folder / "custom_signatures.yml"),
            "--siegfried-home",
            str(tests_folder),
        ],
        standalone_mode=False,
    )

    with FileDB(database_path_copy) as database:
        for file in files:
            file2 = database.files.select(where="uuid = ?", parameters=[str(file.uuid)]).fetchone()
            assert file2
            if file.relative_path.name.split(".", 1)[0].endswith("-encrypted"):
                assert file2.action == "ignore"
                assert file2.action_data.ignore.template == "password-protected"
                assert file2.processed is True
            elif on_success_action := file.action_data.extract.on_success:
                assert file2.action == on_success_action
            else:
                assert file2.action == "ignore"
                assert file2.action_data.ignore.template == "extracted-archive"
                assert file2.processed is True
                assert database.files.select(where="parent = ?", parameters=[str(file.uuid)]).fetchone()


# noinspection DuplicatedCode
def test_doctor_paths(tests_folder: Path, files_folder: Path, files_folder_copy: Path):
    database_path: Path = files_folder / "_metadata" / "files.db"
    database_path_copy: Path = files_folder_copy / database_path.relative_to(files_folder)
    database_path_copy.parent.mkdir(parents=True, exist_ok=True)
    copy(database_path, database_path_copy)

    with FileDB(database_path_copy) as database:
        files: list[File] = list(database.files.select())
        for file in files:
            file.root = files_folder_copy
            file.relative_path = (
                file.get_absolute_path()
                .rename(file.get_absolute_path().with_name("*" + file.name))
                .relative_to(files_folder_copy)
            )
            database.files.update({"relative_path": file.relative_path}, {"uuid": file.uuid})
            database.commit()

    app.main([command_doctor.name, str(files_folder_copy), "--fix", "paths"], standalone_mode=False)

    with FileDB(database_path_copy) as database:
        for file in files:
            file2: File | None = database.files.select(where="uuid = ?", parameters=[str(file.uuid)]).fetchone()
            assert file2
            assert not file.get_absolute_path(files_folder_copy).is_file()
            assert file2.get_absolute_path(files_folder_copy).is_file()
            assert file2.relative_path == file.relative_path.with_name(file.name.replace("*", "_"))


# noinspection DuplicatedCode
def test_doctor_extensions(tests_folder: Path, files_folder: Path, files_folder_copy: Path):
    database_path: Path = files_folder / "_metadata" / "files.db"
    database_path_copy: Path = files_folder_copy / database_path.relative_to(files_folder)
    database_path_copy.parent.mkdir(parents=True, exist_ok=True)
    copy(database_path, database_path_copy)

    with FileDB(database_path_copy) as database:
        files: list[File] = [f for f in database.files.select() if f.suffix]
        for file in files:
            file.root = files_folder_copy
            file.relative_path = (
                file.get_absolute_path()
                .rename(file.get_absolute_path().with_name(file.name + file.suffix))
                .relative_to(files_folder_copy)
            )
            database.files.update({"relative_path": file.relative_path}, {"uuid": file.uuid})
            database.commit()

    app.main([command_doctor.name, str(files_folder_copy), "--fix", "extensions"], standalone_mode=False)

    with FileDB(database_path_copy) as database:
        for file in files:
            file2: File | None = database.files.select(where="uuid = ?", parameters=[str(file.uuid)]).fetchone()
            assert file2
            assert not file.get_absolute_path(files_folder_copy).is_file()
            assert file2.get_absolute_path(files_folder_copy).is_file()
            assert file2.relative_path == file.relative_path.with_name(file.name.removesuffix(file.suffix))


# noinspection DuplicatedCode
def test_history(tests_folder: Path, files_folder: Path):
    app.main(
        [
            command_history.name,
            str(files_folder),
        ],
        standalone_mode=False,
    )

    with pytest.raises(BadParameter):
        app.main(
            [command_history.name, str(files_folder), "--from", "test"],
            standalone_mode=False,
        )

    with pytest.raises(BadParameter):
        app.main(
            [command_history.name, str(files_folder), "--to", "test"],
            standalone_mode=False,
        )

    with pytest.raises(BadParameter):
        app.main(
            [command_history.name, str(files_folder), "--uuid", "test"],
            standalone_mode=False,
        )

    with pytest.raises(BadParameter):
        app.main(
            [command_history.name, str(files_folder), "--operation", "&test"],
            standalone_mode=False,
        )

    app.main(
        [
            command_history.name,
            str(files_folder),
            "--from",
            datetime.fromtimestamp(0).isoformat(),
            "--to",
            datetime.now().isoformat(),
            "--operation",
            f"{app.name}%",
            "--uuid",
            str(uuid4()),
            "--reason",
            "_",
        ],
        standalone_mode=False,
    )


# noinspection DuplicatedCode
def test_edit_action(tests_folder: Path, files_folder: Path, files_folder_copy: Path):
    database_path: Path = files_folder / "_metadata" / "files.db"
    database_path_copy: Path = files_folder_copy / database_path.relative_to(files_folder)
    database_path_copy.parent.mkdir(parents=True, exist_ok=True)
    copy(database_path, database_path_copy)

    with FileDB(database_path_copy) as database:
        file: File = database.files.select(where="puid is not null", limit=1, order_by=[("random()", "asc")]).fetchone()
        assert isinstance(file, File)

    for action in ("convert", "extract", "manual", "ignore"):
        previous_action = file.action
        file.action = action
        action_data: BaseModel
        previous_action_data: BaseModel | None

        if action == "convert":
            previous_action_data = file.action_data.convert if file.action_data.convert else None
            file.action_data.convert = action_data = ConvertAction(tool="test", outputs=["ext"])
        elif action == "extract":
            previous_action_data = file.action_data.extract if file.action_data.extract else None
            file.action_data.extract = action_data = ExtractAction(tool="tool", extension="zip")
        elif action == "manual":
            previous_action_data = file.action_data.manual if file.action_data.manual else None
            file.action_data.manual = action_data = ManualAction(reason="reason", process="process")
        elif action == "ignore":
            previous_action_data = file.action_data.ignore if file.action_data.ignore else None
            file.action_data.ignore = action_data = IgnoreAction(template="not-preservable", reason="reason")
        else:
            continue

        args: list[str] = [
            group_edit.name,
            group_action.name,
            action,
            str(files_folder_copy),
            str(file.uuid),
            action,
            f"edit action {action}",
        ]

        for key, value in action_data.model_dump().items():
            args.append(f"--{key}")
            if isinstance(value, list):
                args.extend(map(str, value))
            else:
                args.append(str(value))

        app.main(args, standalone_mode=False)

        with FileDB(database_path_copy) as database:
            file2: File = database.files.select(where="UUID = ?", limit=1, parameters=[str(file.uuid)]).fetchone()
            assert file2.action_data.model_dump().get(action) == action_data.model_dump()

            history: HistoryEntry = database.history.select(
                where=f"UUID = ? and OPERATION = ?",
                order_by=[("TIME", "desc")],
                limit=1,
                parameters=[str(file.uuid), f"{app.name}.{group_edit.name}.{group_action.name}.{action}:edit"],
            ).fetchone()

            assert history is not None
            assert history.data == [
                previous_action,
                action,
                {action: previous_action_data.model_dump() if previous_action_data else None},
                {action: action_data.model_dump()},
            ]
            assert history.reason == f"edit action {action}"


# noinspection DuplicatedCode
def test_edit_action_ids_file(tests_folder: Path, files_folder: Path, files_folder_copy: Path):
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
        group_edit.name,
        group_action.name,
        "ignore",
        str(files_folder_copy),
        "--uuid",
        "--from-file",
        str(ids_file),
        "--template",
        "not-preservable",
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
            assert file_new.action_data.ignore.template == "not-preservable"

            history_edit: Optional[HistoryEntry] = database.history.select(
                where="uuid = ? and operation like ? || '%'",
                parameters=[str(file.uuid), f"{app.name}.{group_edit.name}.{group_action.name}.ignore:"],
            ).fetchone()
            assert history_edit is not None
            assert history_edit.reason == test_reason


# noinspection DuplicatedCode
def test_edit_rename(files_folder: Path, files_folder_copy: Path):
    database_path: Path = files_folder / "_metadata" / "files.db"
    database_path_copy: Path = files_folder_copy / database_path.relative_to(files_folder)
    database_path_copy.parent.mkdir(parents=True, exist_ok=True)
    copy(database_path, database_path_copy)

    # Ensure the selected file exists and is not one that is renamed by identify
    with FileDB(database_path_copy) as database:
        file_old: File = next(
            f
            for f in database.files.select(order_by=[("random()", "asc")])
            if files_folder.joinpath(f.relative_path).is_file()
        )
        assert isinstance(file_old, File)
        file_old.root = files_folder_copy

    test_extension: str = ".test"
    test_reason: str = "edit extension"

    args: list[str] = [
        group_edit.name,
        command_rename.name,
        "--append",
        str(files_folder_copy),
        "--uuid",
        str(file_old.uuid),
        test_extension,
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


# noinspection DuplicatedCode
def test_edit_rename_same(files_folder: Path, files_folder_copy: Path):
    database_path: Path = files_folder / "_metadata" / "files.db"
    database_path_copy: Path = files_folder_copy / database_path.relative_to(files_folder)
    database_path_copy.parent.mkdir(parents=True, exist_ok=True)
    copy(database_path, database_path_copy)

    # Ensure the selected file exists and is not one that is renamed by identify
    with FileDB(database_path_copy) as database:
        file_old: File = next(
            f
            for f in database.files.select(order_by=[("random()", "asc")])
            if files_folder.joinpath(f.relative_path).is_file() and f.relative_path.suffix
        )
        assert isinstance(file_old, File)
        file_old.root = files_folder_copy

    test_extension: str = file_old.relative_path.suffix
    test_reason: str = "edit extension same"

    args: list[str] = [
        group_edit.name,
        command_rename.name,
        "--replace",
        str(files_folder_copy),
        "--uuid",
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


# noinspection DuplicatedCode
def test_edit_rename_empty(files_folder: Path, files_folder_copy: Path):
    database_path: Path = files_folder / "_metadata" / "files.db"
    database_path_copy: Path = files_folder_copy / database_path.relative_to(files_folder)
    database_path_copy.parent.mkdir(parents=True, exist_ok=True)
    copy(database_path, database_path_copy)

    # Ensure the selected file exists and is not one that is renamed by identify
    with FileDB(database_path_copy) as database:
        file_old: File = next(
            f
            for f in database.files.select(order_by=[("random()", "asc")])
            if files_folder.joinpath(f.relative_path).is_file() and f.relative_path.suffix
        )
        assert isinstance(file_old, File)

    test_reason: str = "edit extension empty"

    args: list[str] = [
        group_edit.name,
        command_rename.name,
        "--uuid",
        str(files_folder_copy),
        str(file_old.uuid),
        "--replace",
        " ",
        test_reason,
    ]

    app.main(args, standalone_mode=False)

    with FileDB(database_path_copy) as database:
        file_new: Optional[File] = database.files.select(where="uuid = ?", parameters=[str(file_old.uuid)]).fetchone()
        assert isinstance(file_new, File)
        file_old.root = files_folder_copy
        file_new.root = files_folder_copy
        assert file_new.relative_path == file_old.relative_path.with_suffix("")
        assert file_new.get_absolute_path().is_file()
        assert not file_old.get_absolute_path().is_file()


# noinspection DuplicatedCode
def test_edit_remove(files_folder: Path, files_folder_copy: Path):
    database_path: Path = files_folder / "_metadata" / "files.db"
    database_path_copy: Path = files_folder_copy / database_path.relative_to(files_folder)
    database_path_copy.parent.mkdir(parents=True, exist_ok=True)
    copy(database_path, database_path_copy)

    with FileDB(database_path_copy) as database:
        file: File = database.files.select(limit=1, order_by=[("random()", "asc")]).fetchone()
        assert isinstance(file, File)

    args: list[str] = [
        group_edit.name,
        command_remove.name,
        "--uuid",
        str(files_folder_copy),
        str(file.uuid),
        "Remove file with uuid",
    ]

    app.main(args, standalone_mode=False)

    with FileDB(database_path_copy) as database:
        file: Optional[File] = database.files.select(where="uuid = ?", limit=1, parameters=[str(file.uuid)]).fetchone()
        assert file is None


# noinspection DuplicatedCode
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
        group_edit.name,
        command_remove.name,
        "--uuid",
        "--from-file",
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


def test_edit_lock(tests_folder: Path, files_folder: Path, files_folder_copy: Path):
    database_path: Path = files_folder / "_metadata" / "files.db"
    database_path_copy: Path = files_folder_copy / database_path.relative_to(files_folder)
    database_path_copy.parent.mkdir(parents=True, exist_ok=True)
    copy(database_path, database_path_copy)

    with FileDB(database_path_copy) as database:
        files: list[File] = list(database.files.select(order_by=[("random()", "asc")], limit=3))

    test_reason: str = "lock"

    args: list[str] = [
        group_edit.name,
        command_lock.name,
        str(files_folder_copy),
        "--uuid",
        *(str(f.uuid) for f in files),
        test_reason,
    ]

    app.main(args, standalone_mode=False)

    with FileDB(database_path_copy) as database:
        for file in files:
            file_new: Optional[File] = database.files.select(where="uuid = ?", parameters=[str(file.uuid)]).fetchone()
            assert file_new is not None
            assert file_new.lock is True

            history_edit: Optional[HistoryEntry] = database.history.select(
                where="uuid = ? and operation like ? || '%'",
                parameters=[str(file.uuid), "digiarch.edit.lock:"],
            ).fetchone()
            assert history_edit is not None
            assert history_edit.reason == test_reason


# noinspection DuplicatedCode
def test_edit_rollback_action(tests_folder: Path, files_folder: Path, files_folder_copy: Path):
    database_path: Path = files_folder / "_metadata" / "files.db"
    database_path_copy: Path = files_folder_copy / database_path.relative_to(files_folder)
    database_path_copy.parent.mkdir(parents=True, exist_ok=True)
    copy(database_path, database_path_copy)

    with FileDB(database_path_copy) as database:
        files: list[File] = list(
            database.files.select(
                where="action!= 'ignore'",
                order_by=[("random()", "asc")],
                limit=3,
            )
        )

    test_reason_edit: str = "action"
    test_reason_rollback: str = "rollback action"
    start_time: datetime = datetime.now()

    app.main(
        [
            group_edit.name,
            group_action.name,
            "ignore",
            str(files_folder_copy),
            *(str(f.uuid) for f in files),
            "ignore",
            test_reason_edit,
            "--template",
            "not-preservable",
        ],
        standalone_mode=False,
    )

    app.main(
        [
            group_edit.name,
            command_rollback.name,
            str(files_folder_copy),
            start_time.isoformat(),
            datetime.now().isoformat(),
            test_reason_rollback,
        ],
        standalone_mode=False,
    )

    with FileDB(database_path_copy) as database:
        for file in files:
            assert database.files.select(
                where="uuid = ? and action = ?",
                parameters=[str(file.uuid), file.action],
            ).fetchone()
            assert database.history.select(
                where="uuid = ? and operation = 'digiarch.edit.rollback:rollback'",
                parameters=[str(file.uuid)],
            ).fetchone()


# noinspection DuplicatedCode
def test_edit_rollback_remove(tests_folder: Path, files_folder: Path, files_folder_copy: Path):
    database_path: Path = files_folder / "_metadata" / "files.db"
    database_path_copy: Path = files_folder_copy / database_path.relative_to(files_folder)
    database_path_copy.parent.mkdir(parents=True, exist_ok=True)
    copy(database_path, database_path_copy)

    with FileDB(database_path_copy) as database:
        files: list[File] = list(database.files.select(order_by=[("random()", "asc")], limit=2))

    test_reason_edit: str = "remove"
    test_reason_rollback: str = "rollback remove"
    start_time: datetime = datetime.now()

    app.main(
        [
            group_edit.name,
            command_remove.name,
            str(files_folder_copy),
            "--uuid",
            *(str(f.uuid) for f in files),
            test_reason_edit,
        ],
        standalone_mode=False,
    )

    app.main(
        [
            group_edit.name,
            command_rollback.name,
            str(files_folder_copy),
            start_time.isoformat(),
            datetime.now().isoformat(),
            test_reason_rollback,
        ],
        standalone_mode=False,
    )

    with FileDB(database_path_copy) as database:
        for file in files:
            assert database.files.select(where="uuid = ?", parameters=[str(file.uuid)]).fetchone()
            assert database.history.select(
                where="uuid = ? and operation = 'digiarch.edit.rollback:rollback'",
                parameters=[str(file.uuid)],
            ).fetchone()


# noinspection DuplicatedCode
def test_edit_rollback_rename(tests_folder: Path, files_folder: Path, files_folder_copy: Path):
    database_path: Path = files_folder / "_metadata" / "files.db"
    database_path_copy: Path = files_folder_copy / database_path.relative_to(files_folder)
    database_path_copy.parent.mkdir(parents=True, exist_ok=True)
    copy(database_path, database_path_copy)

    # Ensure the selected file exists and is not one that is renamed by identify
    with FileDB(database_path_copy) as database:
        files: list[File] = [
            f
            for f in database.files.select(order_by=[("random()", "asc")], limit=3)
            if f.get_absolute_path(files_folder_copy).is_file()
        ]

    test_reason_edit: str = "rename"
    test_reason_rollback: str = "rollback rename"
    start_time: datetime = datetime.now()

    app.main(
        [
            group_edit.name,
            command_rename.name,
            "--uuid",
            str(files_folder_copy),
            *(str(f.uuid) for f in files),
            "--append",
            ".test",
            test_reason_edit,
        ],
        standalone_mode=False,
    )

    app.main(
        [
            group_edit.name,
            command_rollback.name,
            str(files_folder_copy),
            start_time.isoformat(),
            datetime.now().isoformat(),
            test_reason_rollback,
        ],
        standalone_mode=False,
    )

    with FileDB(database_path_copy) as database:
        for file in files:
            file2: File | None = database.files.select(where="uuid = ?", parameters=[str(file.uuid)]).fetchone()
            assert file2
            assert file2.get_absolute_path(files_folder_copy).is_file()
            assert file2.relative_path == file.relative_path
            assert database.history.select(
                where="uuid = ? and operation = 'digiarch.edit.rollback:rollback'",
                parameters=[str(file2.uuid)],
            ).fetchone()


def test_rollback_extract(tests_folder: Path, files_folder: Path, files_folder_copy: Path):
    database_path: Path = files_folder / "_metadata" / "files.db"
    database_path_copy: Path = files_folder_copy / database_path.relative_to(files_folder)
    database_path_copy.parent.mkdir(parents=True, exist_ok=True)
    copy(database_path, database_path_copy)

    # Ensure the selected file exists and is not one that is renamed by identify
    with FileDB(database_path_copy) as database:
        files: list[File] = [
            f
            for f in database.files.select(where="action = 'extract'")
            if f.get_absolute_path(files_folder_copy).is_file()
        ]

    test_reason_rollback: str = "rollback extract"
    start_time: datetime = datetime.now()

    app.main(
        [
            command_extract.name,
            str(files_folder_copy),
            "--actions",
            str(tests_folder / "fileformats.yml"),
            "--custom-signatures",
            str(tests_folder / "custom_signatures.yml"),
            "--siegfried-home",
            str(tests_folder),
        ],
        standalone_mode=False,
    )

    with FileDB(database_path_copy) as database:
        extracted_files: list[File] = [
            f
            for f in database.files.select(where="relative_path like '%/_archive_%/%'")
            if f.get_absolute_path(files_folder_copy).is_file()
        ]

    app.main(
        [
            group_edit.name,
            command_rollback.name,
            str(files_folder_copy),
            start_time.isoformat(),
            datetime.now().isoformat(),
            test_reason_rollback,
        ],
        standalone_mode=False,
    )

    with FileDB(database_path_copy) as database:
        for file in files:
            if file.relative_path.name.split(".", 1)[0].endswith("-encrypted"):
                continue
            file2: File | None = database.files.select(where="uuid = ?", parameters=[str(file.uuid)]).fetchone()
            assert file2
            assert file2.get_absolute_path(files_folder_copy).is_file()
            assert file2.relative_path == file.relative_path
            assert file2.action == file.action
            assert file2.processed is False

        for file in extracted_files:
            file3: File | None = database.files.select(where="uuid = ?", parameters=[str(file.uuid)]).fetchone()
            assert not file3
            assert not file3.get_absolute_path(files_folder_copy).is_file()
