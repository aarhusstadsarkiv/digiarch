from contextlib import chdir
from os import PathLike
from pathlib import Path
from shutil import copy2

import pytest
from acacore.utils.functions import find_files
from acacore.utils.functions import rm_tree
from click import Command


@pytest.fixture(scope="session")
def tests_folder() -> Path:
    return Path(__file__).parent


@pytest.fixture(scope="session")
def avid_folder(tests_folder: Path) -> Path:
    return tests_folder / "AVID"


@pytest.fixture()
def avid_folder_copy(avid_folder: Path, tests_folder: Path) -> Path:
    new_avid_folder: Path = tests_folder / f"_{avid_folder.name}"

    rm_tree(new_avid_folder)

    for file in find_files(avid_folder):
        new_file = new_avid_folder / file.relative_to(avid_folder)
        new_file.parent.mkdir(parents=True, exist_ok=True)
        copy2(file, new_file)

    for directory in [d for d in avid_folder.iterdir() if d.is_dir()]:
        new_avid_folder.joinpath(directory.name).mkdir(parents=True, exist_ok=True)

    return new_avid_folder


@pytest.fixture(scope="session")
def reference_files(tests_folder: Path) -> Path:
    return tests_folder / "reference_files"


def run_click(folder: Path, cmd: Command, *args: str | PathLike | int | float):
    with chdir(folder):
        return cmd.main(list(map(str, args)), standalone_mode=False)
