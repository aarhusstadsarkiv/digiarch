import pytest
from click.testing import CliRunner
from digital_archive.console import cli


@pytest.fixture
def cli_run():
    return CliRunner()


class TestCli:
    def test(self, cli_run):
        with cli_run.isolated_filesystem():
            result = cli_run.invoke(cli, ["report", "--path", "."])
            assert result.exit_code == 0
