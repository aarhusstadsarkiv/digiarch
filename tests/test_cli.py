import pytest
from click.testing import CliRunner
from digiarch.cli import cli


@pytest.fixture
def cli_run():
    return CliRunner()


class TestCli:
    """Class for testing the `cli` function."""

    def test_main_cli_valid(self, cli_run, temp_dir):
        """The cli is run with a valid path as argument.
        This should be successful, i.e. have exit code 0."""
        with cli_run.isolated_filesystem():
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

    def test_main_cli_echos(self, cli_run, temp_dir, data_file):
        """The cli is run given no data file, and so it should collect file
        file information. Afterwards, it is called with an existing data file,
        and so it should echo which file it's working with."""
        with cli_run.isolated_filesystem():
            args = [str(temp_dir)]
            result = cli_run.invoke(cli, args)
            assert "Collecting file information" in result.output
            # Create a data file
            with open(data_file, "w") as file:
                file.write("test")
            result = cli_run.invoke(cli, args)
            assert f"Processing data from {data_file}" in result.output

    def test_reindex_option(self, cli_run, temp_dir, data_file):
        """The cli is run with a data file present, but the --reindex command
        is invoked. Thus, the cli should collect file information anew."""
        with cli_run.isolated_filesystem():
            args = ["--reindex", str(temp_dir)]
            # Create a data file
            with open(data_file, "w") as file:
                file.write("test")
            result = cli_run.invoke(cli, args)
            assert "Collecting file information" in result.output

    def test_report_command(self, cli_run):
        """The cli is run with a valid path as argument and the report option.
        This should be successful, i.e. have exit code 0."""
        with cli_run.isolated_filesystem():
            args = [".", "report"]
            result = cli_run.invoke(cli, args)
            assert result.exit_code == 0
