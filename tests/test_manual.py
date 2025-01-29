from pathlib import Path

from acacore.database import FilesDB
from digiarch.cli import app
from digiarch.common import AVID

from tests.conftest import run_click


# noinspection DuplicatedCode
def test_manual_extract(avid_folder_copy: Path):
    avid = AVID(avid_folder_copy)

    with FilesDB(avid.database_path) as database:
        parent_file = database.original_files.select(
            "parent is null",
            order_by=[("random()", "asc")],
            limit=1,
        ).fetchone()
        assert parent_file

        children = database.original_files.select(
            "parent is null and uuid != ?",
            [str(parent_file.uuid)],
            order_by=[("random()", "asc")],
            limit=3,
        ).fetchall()
        extracted_child = children.pop()
        assert children
        assert extracted_child

        extracted_child.parent = extracted_child.uuid
        database.original_files.update(extracted_child)
        database.commit()

    extra_file_path: Path = avid.dirs.original_documents.joinpath("manual_extract.test")
    extra_file_path.write_text("\n")

    run_click(
        avid.path,
        app,
        "manual",
        "extract",
        parent_file.uuid,
        *(c.get_absolute_path(avid.path) for c in children),
        extracted_child.get_absolute_path(avid.path),
        extra_file_path,
    )

    with FilesDB(avid.database_path) as database:
        for child in children:
            test_file = database.original_files[{"relative_path": str(child.relative_path)}]
            assert test_file
            assert test_file.parent == parent_file.uuid

            event = database.log.select(
                "operation = ? and file_uuid = ?",
                [f"{app.name}.manual.extract:edit", str(child.uuid)],
                limit=1,
            ).fetchone()
            assert event
            assert isinstance(event.data, dict)
            assert event.data["parent"] == str(parent_file.uuid)

        test_file = database.original_files[{"relative_path": str(extracted_child.relative_path)}]
        assert test_file
        assert test_file.parent == extracted_child.uuid

        test_file = database.original_files[{"relative_path": str(extra_file_path.relative_to(avid.path))}]
        assert test_file
        assert test_file.parent == parent_file.uuid
        event = database.log.select(
            "operation = ? and file_uuid = ?",
            [f"{app.name}.manual.extract:new", str(test_file.uuid)],
            limit=1,
        ).fetchone()
        assert event


# noinspection DuplicatedCode
def test_manual_convert_master(avid_folder_copy: Path):
    avid = AVID(avid_folder_copy)

    with FilesDB(avid.database_path) as database:
        original_file = database.original_files.select(order_by=[("random()", "asc")], limit=1).fetchone()
        assert original_file

    converted_file: Path = avid.dirs.master_documents.joinpath("converted.test")
    converted_file.parent.mkdir(parents=True, exist_ok=True)
    converted_file.write_text("\n")

    run_click(avid.path, app, "manual", "convert", original_file.uuid, "master", converted_file)

    with FilesDB(avid.database_path) as database:
        test_file = database.master_files[{"relative_path": str(converted_file.relative_to(avid.path))}]
        assert test_file
        assert test_file.original_uuid == original_file.uuid

        event = database.log.select(
            "operation = ? and file_uuid = ?",
            [f"{app.name}.manual.convert:new", str(test_file.uuid)],
            limit=1,
        ).fetchone()
        assert event
        assert isinstance(event.data, dict)
        assert event.data["original_uuid"] == str(original_file.uuid)


# noinspection DuplicatedCode
def test_manual_convert_access(avid_folder_copy: Path):
    avid = AVID(avid_folder_copy)

    with FilesDB(avid.database_path) as database:
        original_file = database.master_files.select(order_by=[("random()", "asc")], limit=1).fetchone()
        assert original_file

    converted_file: Path = avid.dirs.access_documents.joinpath("converted.test")
    converted_file.parent.mkdir(parents=True, exist_ok=True)
    converted_file.write_text("\n")

    run_click(avid.path, app, "manual", "convert", original_file.uuid, "access", converted_file)

    with FilesDB(avid.database_path) as database:
        test_file = database.access_files[{"relative_path": str(converted_file.relative_to(avid.path))}]
        assert test_file
        assert test_file.original_uuid == original_file.uuid

        event = database.log.select(
            "operation = ? and file_uuid = ?",
            [f"{app.name}.manual.convert:new", str(test_file.uuid)],
            limit=1,
        ).fetchone()
        assert event
        assert isinstance(event.data, dict)
        assert event.data["original_uuid"] == str(original_file.uuid)


# noinspection DuplicatedCode
def test_manual_convert_statutory(avid_folder_copy: Path):
    avid = AVID(avid_folder_copy)

    with FilesDB(avid.database_path) as database:
        original_file = database.master_files.select(order_by=[("random()", "asc")], limit=1).fetchone()
        assert original_file

    converted_file: Path = avid.dirs.documents.joinpath("converted.test")
    converted_file.parent.mkdir(parents=True, exist_ok=True)
    converted_file.write_text("\n")

    run_click(avid.path, app, "manual", "convert", original_file.uuid, "statutory", converted_file)

    with FilesDB(avid.database_path) as database:
        test_file = database.statutory_files[{"relative_path": str(converted_file.relative_to(avid.path))}]
        assert test_file
        assert test_file.original_uuid == original_file.uuid

        event = database.log.select(
            "operation = ? and file_uuid = ?",
            [f"{app.name}.manual.convert:new", str(test_file.uuid)],
            limit=1,
        ).fetchone()
        assert event
        assert isinstance(event.data, dict)
        assert event.data["original_uuid"] == str(original_file.uuid)
