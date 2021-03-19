# This is how to get version information
# NB! Doesn't work with pyinstaller
from importlib.metadata import version  # type: ignore

__version__ = version("digiarch")
