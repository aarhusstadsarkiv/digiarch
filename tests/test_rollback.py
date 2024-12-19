from pathlib import Path

from acacore.database import FilesDB

from digiarch.cli import app
from digiarch.common import AVID

from .conftest import run_click


# noinspection DuplicatedCode
def test_rollback_extract(reference_files: Path, avid_folder_copy: Path):
    avid = AVID(avid_folder_copy)

    with FilesDB(avid.database_path) as database:
        files = database.original_files.select("action = 'extract'").fetchall()

    run_click(avid.path, app, "extract", "--siegfried-home", reference_files)

    with FilesDB(avid.database_path) as database:
        extracted_files = database.original_files.select("parent is not null").fetchall()
        for f in files:
            assert database.original_files[{"uuid": str(f.uuid)}].action != "extract"
        for f in extracted_files:
            assert f.get_absolute_path(avid.path).is_file()

    run_click(avid.path, app, "edit", "rollback", 1)

    with FilesDB(avid.database_path) as database:
        for f in files:
            assert database.original_files[{"uuid": str(f.uuid)}].action == "extract", repr(f)
        for f in extracted_files:
            assert database.original_files[{"uuid": str(f.uuid)}] is None
            assert not f.get_absolute_path(avid.path).is_file()
        assert not database.original_files.select("parent is not null").fetchall()


def test_rollback_edit_original_action_convert(avid_folder_copy: Path):
    avid = AVID(avid_folder_copy)

    with FilesDB(avid.database_path) as database:
        files = database.original_files.select("action != 'convert'").fetchall()

    run_click(
        avid.path,
        app,
        "edit",
        "original",
        "action",
        "convert",
        f"@uuid {' '.join(str(f.uuid) for f in files)}",
        "test",
        "--tool",
        "test-tool",
        "--output",
        "test-output",
    )

    with FilesDB(avid.database_path) as database:
        for f in files:
            f_new = database.original_files[{"uuid": str(f.uuid)}]
            assert f_new.action == "convert"
            assert f_new.action_data.convert
            assert f_new.action_data.convert.tool == "test-tool"
            assert f_new.action_data.convert.output == "test-output"

    run_click(avid.path, app, "edit", "rollback", 1)

    with FilesDB(avid.database_path) as database:
        for f in files:
            f_new = database.original_files[{"uuid": str(f.uuid)}]
            assert f_new.action == f.action
            assert f_new.action_data.convert == f.action_data.convert


# noinspection DuplicatedCode
def test_rollback_edit_original_processed(avid_folder_copy: Path):
    avid = AVID(avid_folder_copy)

    with FilesDB(avid.database_path) as database:
        files = database.original_files.select("not processed").fetchall()

    run_click(
        avid.path,
        app,
        "edit",
        "original",
        "processed",
        f"@uuid {' '.join(str(f.uuid) for f in files)}",
        "--processed",
        "test",
    )

    with FilesDB(avid.database_path) as database:
        for f in files:
            assert database.original_files[{"uuid": str(f.uuid)}].processed

    run_click(avid.path, app, "edit", "rollback", 1)

    with FilesDB(avid.database_path) as database:
        for f in files:
            assert not database.original_files[{"uuid": str(f.uuid)}].processed


# noinspection DuplicatedCode
def test_rollback_edit_original_lock(avid_folder_copy: Path):
    avid = AVID(avid_folder_copy)

    with FilesDB(avid.database_path) as database:
        files = database.original_files.select("not lock").fetchall()

    run_click(
        avid.path, app, "edit", "original", "lock", f"@uuid {' '.join(str(f.uuid) for f in files)}", "--lock", "test"
    )

    with FilesDB(avid.database_path) as database:
        for f in files:
            assert database.original_files[{"uuid": str(f.uuid)}].lock

    run_click(avid.path, app, "edit", "rollback", 1)

    with FilesDB(avid.database_path) as database:
        for f in files:
            assert not database.original_files[{"uuid": str(f.uuid)}].lock


# noinspection DuplicatedCode
def test_rollback_edit_original_rename(avid_folder_copy: Path):
    avid = AVID(avid_folder_copy)

    with FilesDB(avid.database_path) as database:
        files = database.original_files.select().fetchmany(2)

    run_click(
        avid.path,
        app,
        "edit",
        "original",
        "rename",
        f"@uuid {' '.join(str(f.uuid) for f in files)}",
        ".test",
        "test",
    )

    with FilesDB(avid.database_path) as database:
        for f in files:
            assert database.original_files[{"uuid": str(f.uuid)}]
            assert not f.get_absolute_path(avid.path).is_file()
            assert f.get_absolute_path(avid.path).with_name(f.name + ".test").is_file()

    run_click(avid.path, app, "edit", "rollback", 1)

    with FilesDB(avid.database_path) as database:
        for f in files:
            f_new = database.original_files[{"uuid": str(f.uuid)}]
            assert f_new
            assert f_new.relative_path == f.relative_path
            assert f.get_absolute_path(avid.path).is_file()


# noinspection DuplicatedCode
def test_rollback_edit_original_remove(avid_folder_copy: Path):
    avid = AVID(avid_folder_copy)

    with FilesDB(avid.database_path) as database:
        files = database.original_files.select().fetchmany(2)

    run_click(avid.path, app, "edit", "original", "remove", f"@uuid {' '.join(str(f.uuid) for f in files)}", "test")

    with FilesDB(avid.database_path) as database:
        for f in files:
            assert database.original_files[{"uuid": str(f.uuid)}] is None
            assert f.get_absolute_path(avid.path).is_file()

    run_click(avid.path, app, "edit", "rollback", 1)

    with FilesDB(avid.database_path) as database:
        for f in files:
            assert database.original_files[{"uuid": str(f.uuid)}] is not None
            assert f.get_absolute_path(avid.path).is_file()


# noinspection DuplicatedCode
def test_rollback_edit_original_remove_delete(avid_folder_copy: Path):
    avid = AVID(avid_folder_copy)

    with FilesDB(avid.database_path) as database:
        files = database.original_files.select().fetchmany(2)

    run_click(
        avid.path,
        app,
        "edit",
        "original",
        "remove",
        "--delete",
        f"@uuid {' '.join(str(f.uuid) for f in files)}",
        "test",
    )

    with FilesDB(avid.database_path) as database:
        for f in files:
            assert database.original_files[{"uuid": str(f.uuid)}] is None
            assert not f.get_absolute_path(avid.path).is_file()

    run_click(avid.path, app, "edit", "rollback", 1)

    with FilesDB(avid.database_path) as database:
        for f in files:
            assert database.original_files[{"uuid": str(f.uuid)}] is None
            assert not f.get_absolute_path(avid.path).is_file()
