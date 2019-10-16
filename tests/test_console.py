import pytest
from click.testing import CliRunner
from digital_archive.console import cli


@pytest.fixture
def cli_run():
    return CliRunner()


class TestCli:
    """Class for testing the `cli` function."""

    def test_report(self, cli_run):
        """The `report` command is called with a valid path.
        This should be successful, i.e. have exit code 0."""
        with cli_run.isolated_filesystem():
            args = ["report", "--path", "."]
            result = cli_run.invoke(cli, args)
            assert result.exit_code == 0
