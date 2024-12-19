from asyncio import Event
from pathlib import Path

from acacore.database import FilesDB
from acacore.models.file import OriginalFile
from acacore.models.reference_files import ConvertAction
from acacore.models.reference_files import ExtractAction
from acacore.models.reference_files import IgnoreAction
from acacore.models.reference_files import ManualAction
from pydantic import BaseModel

from digiarch.cli import app
from digiarch.common import AVID
from tests.conftest import run_click


# noinspection DuplicatedCode
def test_edit_original_action(tests_folder: Path, avid_folder_copy: Path):
    avid = AVID(avid_folder_copy)

    with FilesDB(avid.database_path) as database:
        base_file = database.original_files.select(
            where="puid is not null",
            limit=1,
            order_by=[("random()", "asc")],
        ).fetchone()
        assert isinstance(base_file, OriginalFile)

    for action in ("convert", "extract", "manual", "ignore"):
        previous_action = base_file.action
        base_file.action = action
        action_data: BaseModel
        previous_action_data: BaseModel | None

        if action == "convert":
            previous_action_data = base_file.action_data.convert if base_file.action_data.convert else None
            base_file.action_data.convert = action_data = ConvertAction(tool="test", output="ext")
        elif action == "extract":
            previous_action_data = base_file.action_data.extract if base_file.action_data.extract else None
            base_file.action_data.extract = action_data = ExtractAction(tool="tool", extension="zip")
        elif action == "manual":
            previous_action_data = base_file.action_data.manual if base_file.action_data.manual else None
            base_file.action_data.manual = action_data = ManualAction(reason="reason", process="process")
        elif action == "ignore":
            previous_action_data = base_file.action_data.ignore if base_file.action_data.ignore else None
            base_file.action_data.ignore = action_data = IgnoreAction(template="not-preservable", reason="reason")
        else:
            continue

        reason: str = f"edit action {action}"
        args: list[str] = [
            "edit",
            "original",
            "action",
            action,
            f"@uuid {base_file.uuid}",
            reason,
        ]

        for key, value in action_data.model_dump().items():
            args.append(f"--{key}")
            if isinstance(value, list):
                args.extend(map(str, value))
            else:
                args.append(str(value))

        run_click(avid.path, app, *args)

        with FilesDB(avid.database_path) as database:
            test_file = database.original_files.select("UUID = ?", [str(base_file.uuid)], limit=1).fetchone()
            assert test_file.action_data.model_dump().get(action) == action_data.model_dump()

            event: Event = database.log.select(
                "file_uuid = ? and operation = ?",
                [str(base_file.uuid), f"{app.name}.edit.original.action.{action}:edit"],
                [("TIME", "desc")],
                1,
            ).fetchone()

            assert event is not None
            assert event.data == [
                previous_action,
                action,
                {action: previous_action_data.model_dump() if previous_action_data else None},
                {action: action_data.model_dump()},
            ]
            assert event.reason == reason


def test_edit_original_rename(avid_folder_copy: Path):
    avid = AVID(avid_folder_copy)

    with FilesDB(avid.database_path) as database:
        base_file = next(f for f in database.original_files.select() if avid.path.joinpath(f.relative_path).is_file())
        assert base_file is not None

    extension: str = ".test"
    reason: str = "edit extension"

    args: list[str] = [
        "edit",
        "original",
        "rename",
        "--append",
        f"@uuid {base_file.uuid}",
        extension,
        reason,
    ]

    run_click(avid.path, app, *args)

    with FilesDB(avid.database_path) as database:
        test_file = database.original_files[{"uuid": str(base_file.uuid)}]
        assert test_file is not None
        assert test_file.name == base_file.name + extension
        assert test_file.get_absolute_path(avid.path).is_file()
        assert not base_file.get_absolute_path(avid.path).is_file()

        event = database.log.select(
            "file_uuid = ? and operation = ?",
            [str(base_file.uuid), f"{app.name}.edit.original.rename:edit"],
        ).fetchone()
        assert event is not None
        assert event.reason == reason
        assert event.data == [base_file.name, test_file.name]


def test_edit_original_rename_same(avid_folder_copy: Path):
    avid = AVID(avid_folder_copy)

    with FilesDB(avid.database_path) as database:
        base_file = next(
            f for f in database.original_files.select() if avid.path.joinpath(f.relative_path).is_file() and f.suffix
        )
        assert base_file is not None

    args: list[str] = [
        "edit",
        "original",
        "rename",
        "--append",
        f"@uuid {base_file.uuid}",
        base_file.suffix,
        "rename",
    ]

    run_click(avid.path, app, *args)

    with FilesDB(avid.database_path) as database:
        test_file = database.original_files[{"uuid": str(base_file.uuid)}]
        assert test_file is not None
        assert test_file.relative_path == base_file.relative_path
        assert test_file.get_absolute_path(avid.path).is_file()

        event = database.log.select(
            "file_uuid = ? and operation = ?",
            [str(base_file.uuid), f"{app.name}.edit.original.rename:edit"],
        ).fetchone()
        assert event is None


def test_edit_original_rename_empty(avid_folder_copy: Path):
    avid = AVID(avid_folder_copy)

    with FilesDB(avid.database_path) as database:
        base_file = next(
            f for f in database.original_files.select() if avid.path.joinpath(f.relative_path).is_file() and f.suffix
        )
        assert base_file is not None

    args: list[str] = [
        "edit",
        "original",
        "rename",
        "--replace",
        f"@uuid {base_file.uuid}",
        " ",
        "remove extension",
    ]

    run_click(avid.path, app, *args)

    with FilesDB(avid.database_path) as database:
        test_file = database.original_files[{"uuid": str(base_file.uuid)}]
        assert test_file is not None
        assert test_file.relative_path == base_file.relative_path.with_suffix("")
        assert test_file.get_absolute_path(avid.path).is_file()
        assert not base_file.get_absolute_path().is_file()


def test_edit_original_remove(avid_folder_copy: Path):
    avid = AVID(avid_folder_copy)
    reason: str = "remove file"

    with FilesDB(avid.database_path) as database:
        base_file = database.original_files.select(order_by=[("random()", "asc")], limit=1).fetchone()
        assert base_file is not None

    run_click(avid.path, app, "edit", "original", "remove", f"@uuid {base_file.uuid}", reason)

    with FilesDB(avid.database_path) as database:
        assert database.original_files[{"uuid": str(base_file.uuid)}] is None
        assert database.original_files[{"relative_path": str(base_file.relative_path)}] is None

        event = database.log.select(
            "file_uuid = ? and operation = ?",
            [str(base_file.uuid), f"{app.name}.edit.original.remove:remove"],
        ).fetchone()
        assert event is not None
        assert event.reason == reason
        assert event.data == base_file.model_dump(mode="json")


def test_edit_original_remove_delete(avid_folder_copy: Path):
    avid = AVID(avid_folder_copy)
    reason: str = "remove file"

    with FilesDB(avid.database_path) as database:
        base_file = database.original_files.select(order_by=[("random()", "asc")], limit=1).fetchone()
        assert base_file is not None

    run_click(avid.path, app, "edit", "original", "remove", f"@uuid {base_file.uuid}", reason, "--delete")

    with FilesDB(avid.database_path) as database:
        assert database.original_files[{"uuid": str(base_file.uuid)}] is None
        assert database.original_files[{"relative_path": str(base_file.relative_path)}] is None
        assert not base_file.get_absolute_path(avid.path).is_file()

        event = database.log.select(
            "file_uuid = ? and operation = ?",
            [str(base_file.uuid), f"{app.name}.edit.original.remove:delete"],
        ).fetchone()
        assert event is not None
        assert event.reason == reason
        assert event.data == base_file.model_dump(mode="json")


# noinspection DuplicatedCode
def test_edit_original_lock(avid_folder_copy: Path):
    avid = AVID(avid_folder_copy)
    reason: str = "lock file"

    with FilesDB(avid.database_path) as database:
        base_file = database.original_files.select(order_by=[("random()", "asc")], limit=1).fetchone()
        assert base_file is not None

    run_click(avid.path, app, "edit", "original", "lock", f"@uuid {base_file.uuid}", reason, "--lock")

    with FilesDB(avid.database_path) as database:
        test_file = database.original_files[{"uuid": str(base_file.uuid)}]
        assert test_file is not None
        assert test_file.lock

        event = database.log.select(
            "file_uuid = ? and operation = ?",
            [str(base_file.uuid), f"{app.name}.edit.original.lock:edit"],
        ).fetchone()
        assert event is not None
        assert event.reason == reason
        assert event.data == [base_file.lock, test_file.lock]


# noinspection DuplicatedCode
def test_edit_original_processed(avid_folder_copy: Path):
    avid = AVID(avid_folder_copy)
    reason: str = "process file"

    with FilesDB(avid.database_path) as database:
        base_file = database.original_files.select("not processed", order_by=[("random()", "asc")], limit=1).fetchone()
        assert base_file is not None

    run_click(avid.path, app, "edit", "original", "processed", f"@uuid {base_file.uuid}", reason, "--processed")

    with FilesDB(avid.database_path) as database:
        test_file = database.original_files[{"uuid": str(base_file.uuid)}]
        assert test_file is not None
        assert test_file.processed

        event = database.log.select(
            "file_uuid = ? and operation = ?",
            [str(base_file.uuid), f"{app.name}.edit.original.processed:edit"],
        ).fetchone()
        assert event is not None
        assert event.reason == reason
        assert event.data == [False, True]

    run_click(avid.path, app, "edit", "original", "processed", f"@uuid {base_file.uuid}", reason)

    with FilesDB(avid.database_path) as database:
        test_file = database.original_files[{"uuid": str(base_file.uuid)}]
        assert test_file is not None
        assert test_file.processed

        assert (
            database.log.select(
                "file_uuid = ? and operation = ? and time > ?",
                [str(base_file.uuid), f"{app.name}.edit.original.processed:edit", event.time.isoformat()],
            ).fetchone()
            is None
        )

    run_click(avid.path, app, "edit", "original", "processed", f"@uuid {base_file.uuid}", reason, "--unprocessed")

    with FilesDB(avid.database_path) as database:
        test_file = database.original_files[{"uuid": str(base_file.uuid)}]
        assert test_file is not None
        assert not test_file.processed

        event = database.log.select(
            "file_uuid = ? and operation = ?",
            [str(base_file.uuid), f"{app.name}.edit.original.processed:edit"],
            order_by=[("time", "desc")],
        ).fetchone()
        assert event is not None
        assert event.reason == reason
        assert event.data == [True, False]
