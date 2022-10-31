# This is how to get version information
# NB! Doesn't work with pyinstaller
# from importlib.metadata import version  # type: ignore
from pathlib import Path


def get_version() -> str:
    version: str = "Ukendt version"
    with open(Path(__file__).absolute().parent.parent / "pyproject.toml") as i:
        for line in i.readlines():
            if line.startswith("version"):
                version = line[line.index('"') + 1 : -2]
    return version


# __version__ = "0.9.18"
__version__ = get_version()
