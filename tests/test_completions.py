from pathlib import Path

from click.shell_completion import BashComplete
from click.shell_completion import FishComplete
from click.shell_completion import ZshComplete
from digiarch.cli import app

from tests.conftest import run_click


def test_completions(tests_folder: Path):
    for shell in (BashComplete, FishComplete, ZshComplete):
        run_click(tests_folder, app, "completions", shell.name)
