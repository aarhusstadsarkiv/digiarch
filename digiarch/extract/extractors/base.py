from abc import ABC
from abc import abstractmethod
from pathlib import Path
from shutil import copy2
from typing import Generator

from acacore.database import FileDB
from acacore.exceptions.base import ACAException
from acacore.models.file import File


class ExtractError(ACAException):
    """Base class for unarchiver exceptions"""

    def __init__(self, file: File, msg: str = "Extraction error"):
        self.file: File = file
        super().__init__(msg)


class PasswordProtectedError(ExtractError):
    """Archive is encrypted."""


class ExtractorBase(ABC):
    tool_names: list[str]

    def __init__(self, database: FileDB, file: File, root: Path | None = None):
        self.database: FileDB = database
        self.file: File = file
        self.file.root = root or self.file.root or database.path.parent.parent

    @property
    def extract_folder(self):
        return self.file.get_absolute_path().parent / f"_archive_{self.file.name}"

    @abstractmethod
    def extract(self) -> Generator[Path, None, None]: ...
