# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from digiarch.cli import cli

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

    def test_main_cli_valid(self, cli_run, temp_dir):
        """The cli is run with a valid path as argument.
        This should be successful, i.e. have exit code 0."""
        with cli_run.isolated_filesystem():
            Path(temp_dir, "test.txt").touch()
            args = [str(temp_dir)]
            result = cli_run.invoke(cli, args)
            assert result.exit_code == 0

    def test_main_cli_invalid(self, cli_run):
        """The cli is run with an invalid path as argument.
        This should fail horribly, i.e. exit code != 0."""
        with cli_run.isolated_filesystem():
            args = ["/fail/"]
            result = cli_run.invoke(cli, args)
            assert result.exit_code != 0

    def test_main_cli_echos(self, cli_run, temp_dir, file_data):
        """The cli is run given no data file, and so it should collect file
        information. Afterwards, it is called with an existing data file,
        and so it should echo which file it's working with."""
        with cli_run.isolated_filesystem():
            args = [str(temp_dir)]
            # Create an empty directory
            empty_dir = temp_dir / "empty"
            empty_dir.mkdir()
            result = cli_run.invoke(cli, args)
            assert "Warning! Empty subdirectories detected!" in result.output
            assert "Collecting file information" in result.output
            # Create a data file
            new_file = file_data
            new_file.to_json()
            result = cli_run.invoke(cli, args)
            assert (
                f"Processing data from {new_file.json_file}" in result.output
            )
            # Create several files in one folder
            args = ["--reindex", str(temp_dir)]
            file_1 = temp_dir / "file1.txt"
            file_2 = temp_dir / "file2.txt"
            file_1.touch()
            file_2.touch()
            result = cli_run.invoke(cli, args)
            assert (
                "Warning! Some directories have several files!"
                in result.output
            )

    def test_reindex_option(self, cli_run, temp_dir, data_file):
        """The cli is run with a data file present, but the --reindex command
        is invoked. Thus, the cli should collect file information anew."""
        with cli_run.isolated_filesystem():
            args = ["--reindex", str(temp_dir)]
            # Create a data file
            json.dump({"test": "test"}, data_file.open("w"))
            result = cli_run.invoke(cli, args)
            assert "Collecting file information" in result.output

    def test_all_option(self, cli_run, temp_dir, data_file):
        with cli_run.isolated_filesystem():
            args = ["--all", str(temp_dir)]
            Path(temp_dir, "test.txt").touch()
            result = cli_run.invoke(cli, args)
            print(result.output)
            assert "Generating checksums" in result.output
            assert "Identifying files" in result.output
            assert "Creating reports" in result.output
            assert "Grouping files" in result.output
            assert "Finding duplicates" in result.output

    def test_report_command(self, cli_run, temp_dir):
        """The cli is run with a valid path as argument and the report option.
        This should be successful, i.e. have exit code 0."""
        with cli_run.isolated_filesystem():
            Path(temp_dir, "test.txt").touch()
            args = [str(temp_dir), "report"]
            result = cli_run.invoke(cli, args)
            print(result.stdout)
            assert result.exit_code == 0

    def test_group_command(self, cli_run, temp_dir):
        with cli_run.isolated_filesystem():
            Path(temp_dir, "test.txt").touch()
            args = [str(temp_dir), "group"]
            result = cli_run.invoke(cli, args)
            assert result.exit_code == 0

    def test_checksum_command(self, cli_run, temp_dir):
        with cli_run.isolated_filesystem():
            Path(temp_dir, "test.txt").touch()
            args = [str(temp_dir), "checksum"]
            result = cli_run.invoke(cli, args)
            assert result.exit_code == 0

    def test_dups_command(self, cli_run, temp_dir):
        with cli_run.isolated_filesystem():
            Path(temp_dir, "test.txt").touch()
            args = [str(temp_dir), "dups"]
            result = cli_run.invoke(cli, args)
            assert result.exit_code == 0

    def test_identify_command(self, cli_run, temp_dir):
        Path(temp_dir, "test.txt").touch()
        args = [str(temp_dir), "identify"]
        result = cli_run.invoke(cli, args)
        assert result.exit_code == 0
