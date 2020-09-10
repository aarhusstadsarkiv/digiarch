# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------


from pathlib import Path

import pytest
from click.testing import CliRunner
from acamodels import ArchiveFile

from digiarch.cli import cli
from digiarch.exceptions import FileCollectionError, IdentificationError
from digiarch import core

# -----------------------------------------------------------------------------
# Fixtures
# -----------------------------------------------------------------------------


@pytest.fixture
def cli_run():
    return CliRunner()


# -----------------------------------------------------------------------------
# Tests
# -----------------------------------------------------------------------------


class TestCli:
    """Class for testing the `cli` function."""

    def test_cli_valid(self, cli_run, temp_dir):
        """The cli is run with a valid path as argument.
        This should be successful, i.e. have exit code 0."""
        Path(temp_dir, "test.txt").touch()
        with cli_run.isolated_filesystem():
            args = [str(temp_dir)]
            result = cli_run.invoke(cli, args)
            print(result.exc_info)
            assert result.exit_code == 0

    def test_cli_invalid(self, cli_run):
        """The cli is run with an invalid path as argument.
        This should fail horribly, i.e. exit code != 0."""
        with cli_run.isolated_filesystem():
            args = ["/fail/"]
            result = cli_run.invoke(cli, args)
            assert result.exit_code != 0

    def test_exceptions(self, cli_run, monkeypatch, temp_dir):
        def file_coll_error(*args):
            raise FileCollectionError("File Collection Error")

        monkeypatch.setattr(core, "explore_dir", file_coll_error)
        Path(temp_dir, "test.txt").touch()
        with cli_run.isolated_filesystem():
            args = [str(temp_dir)]
            result = cli_run.invoke(cli, args)
            assert "Error: File Collection Error" in result.output

    def test_cli_echos(self, cli_run, temp_dir, file_data, monkeypatch):
        """Runs the CLI with empty directories, multiple files, and
        an already existing database."""
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
            assert (
                "Warning! Some directories have multiple files!"
                in result.output
            )

            # File database has data
            monkeypatch.setattr(file_data.db, "is_empty", lambda: False)
            result = cli_run.invoke(cli, args)
            assert "Processing data from" in result.output


class TestOptions:
    def test_reindex(self, cli_run, temp_dir, file_data, monkeypatch):
        Path(temp_dir, "test.txt").touch()
        args = ["--reindex", str(temp_dir)]
        # File database has data
        monkeypatch.setattr(file_data.db, "is_empty", lambda: False)

        with cli_run.isolated_filesystem():
            # But we pass reindex, so file collection should happen anyway
            result = cli_run.invoke(cli, args)
            assert "Collecting file information" in result.output


class TestCommands:
    def test_process(self, cli_run, temp_dir, monkeypatch):
        def id_error(*args):
            raise IdentificationError("Identification Error")

        args = [str(temp_dir), "process"]
        Path(temp_dir, "test.txt").touch()

        with cli_run.isolated_filesystem():
            result = cli_run.invoke(cli, args)
            assert result.exit_code == 0
            assert "Successfully identified 1 files" in result.output

            monkeypatch.setattr(core, "identify", id_error)
            result = cli_run.invoke(cli, args)
            assert "Error: Identification Error" in result.output

    def test_fix(self, cli_run, temp_dir, monkeypatch, xls_info):
        Path(temp_dir, "test.txt").touch()
        args = [str(temp_dir), "fix"]

        with cli_run.isolated_filesystem():
            monkeypatch.setattr(core, "fix_extensions", lambda *args: [])
            result = cli_run.invoke(cli, args)
            assert "Info: No file extensions to fix" in result.output

            monkeypatch.setattr(
                core,
                "fix_extensions",
                lambda *args: [ArchiveFile(path=xls_info)],
            )
            result = cli_run.invoke(cli, args)
            assert "Rebuilding file information" in result.output
