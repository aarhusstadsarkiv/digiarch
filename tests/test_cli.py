# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------

import os
from pathlib import Path

import pytest
from click.testing import CliRunner

from digiarch import core
from digiarch.cli import cli, get_preservable_info
from digiarch.database import db
from digiarch.exceptions import FileCollectionError, FileParseError, IdentificationError
from digiarch.models import FileData

# -----------------------------------------------------------------------------
# Fixtures
# -----------------------------------------------------------------------------


@pytest.fixture()
def cli_run():
    return CliRunner()


@pytest.fixture()
def file_data():
    _file_data: FileData = FileData(main_dir=Path.cwd(), files=[])
    yield _file_data
    file_db_path = Path.cwd() / "_metadata" / "files.db"
    file_db_path.unlink()
    file_db_path.parent.rmdir()


# -----------------------------------------------------------------------------
# Tests
# -----------------------------------------------------------------------------


class TestCli:
    """Class for testing the `cli` function."""

    def test_cli_valid(self, cli_run):
        with cli_run.isolated_filesystem():
            os.environ["ROOTPATH"] = str(Path.cwd())
            testdir = Path.cwd() / "testdir"
            testdir.mkdir()
            testdirfile = Path.cwd() / "testdir" / "test.txt"
            testdirfile.touch()
            result = cli_run.invoke(cli, [os.environ["ROOTPATH"]])
            assert result.exit_code == 0

    def test_cli_invalid(self, cli_run):
        """The cli is run with an invalid path as argument.

        This should fail horribly, i.e. exit code != 0.
        """
        with cli_run.isolated_filesystem():
            args = ["/fail/"]
            result = cli_run.invoke(cli, args)
            assert result.exit_code != 0

    def test_exceptions(self, cli_run, monkeypatch, temp_dir):
        def file_coll_error(*args):
            raise FileCollectionError("File Collection Error")

        def file_parse_error(*args):
            raise FileParseError("File Parse Error")

        Path(temp_dir, "test.txt").touch()
        args = [str(temp_dir)]
        with cli_run.isolated_filesystem():
            monkeypatch.setattr(core, "explore_dir", file_coll_error)
            result = cli_run.invoke(cli, args)
            assert "Error: File Collection Error" in result.output
            monkeypatch.undo()
        with cli_run.isolated_filesystem():
            monkeypatch.setattr(db.FileDB, "get_files", file_parse_error)
            result = cli_run.invoke(cli, args)
            assert "Error: File Parse Error" in result.output

    def test_cli_echos(self, cli_run, temp_dir, monkeypatch):
        """Runs the CLI with empty directories, multiple files, and an already existing database."""
        args = [str(temp_dir)]
        with cli_run.isolated_filesystem():
            # Create an empty directory
            empty_dir = temp_dir / "empty"
            empty_dir.mkdir()
            result = cli_run.invoke(cli, args)
            assert "Warning! Empty subdirectories detected!" in result.output
            assert "Collecting file information" in result.output

            # Create several files in one folder
            file_1 = temp_dir / "file1.txt"
            file_2 = temp_dir / "file2.txt"
            file_1.touch()
            file_2.touch()
            result = cli_run.invoke(cli, args)
            assert "Warning! Some directories have multiple files!" in result.output

        with cli_run.isolated_filesystem():
            # File database has data
            async def empty_false(*args):
                return False

            monkeypatch.setattr(db.FileDB, "is_empty", empty_false)
            result = cli_run.invoke(cli, args)
            assert "Processing data from" in result.output


class TestOptions:
    def test_reindex(self, cli_run, temp_dir, monkeypatch):
        Path(temp_dir, "test.txt").touch()
        args = ["--reindex", str(temp_dir)]

        async def empty_false(*args):
            return False

        # File database has data
        monkeypatch.setattr(db.FileDB, "is_empty", empty_false)

        with cli_run.isolated_filesystem():
            # But we pass reindex, so file collection should happen anyway
            result = cli_run.invoke(cli, args)
            assert "Collecting file information" in result.output


class TestCommands:
    def test_process(self, cli_run, monkeypatch):
        def id_error(*args):
            raise IdentificationError("Identification Error")

        with cli_run.isolated_filesystem():

            os.environ["ROOTPATH"] = str(Path.cwd())
            testdir = Path.cwd() / "testdir"
            testdir.mkdir()
            testdirfile = Path.cwd() / "testdir" / "test.txt"
            testdirfile.touch()
            result = cli_run.invoke(cli, [os.environ["ROOTPATH"]])

            args = [os.environ["ROOTPATH"], "process"]
            result = cli_run.invoke(cli, args)
            assert result.exit_code == 0
            assert "Successfully identified 1 files" in result.output

            monkeypatch.setattr(core, "identify", id_error)
            result = cli_run.invoke(cli, args)
            assert "Error: Identification Error" in result.output


class TestPreservableInfo:
    def test_get_preservable_info(self, very_small_binary_file, file_data):
        file_data.files.append(very_small_binary_file)
        information = get_preservable_info(file_data)
        assert len(information) == 1
        assert information[0]["uuid"] == str(very_small_binary_file.uuid)
        assert information[0]["ignore reason"] == "Binary file is less than 1 kb."
