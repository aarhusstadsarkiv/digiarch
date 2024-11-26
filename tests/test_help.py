from pathlib import Path
from typing import Generator

import pytest
from click import Group
from click import UsageError

from digiarch.cli import app
from digiarch.common import AVID
from tests.conftest import run_click


def find_commands(group: Group) -> Generator[list[str], None, None]:
    for name, command in group.commands.items():
        if isinstance(command, Group):
            yield from ([name, *cmds] for cmds in find_commands(command))
        else:
            yield [name]


def test_help(avid_folder_copy: Path):
    avid = AVID(avid_folder_copy)
    size: int = avid.database_path.stat().st_size
    m_time: float = avid.database_path.stat().st_mtime

    for command in find_commands(app):
        run_click(avid.path, app, "help", *command)

    assert avid.database_path.stat().st_size == size
    assert avid.database_path.stat().st_mtime == m_time


def test_help_invalid():
    with pytest.raises(UsageError, match="No such command 'invalid-command'"):
        run_click(Path.cwd(), app, "help", "invalid-command")
