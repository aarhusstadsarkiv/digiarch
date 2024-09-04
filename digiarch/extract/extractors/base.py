from abc import ABC
from abc import abstractmethod
from pathlib import Path
from typing import ClassVar
from typing import Generator

from acacore.database import FileDB
from acacore.exceptions.base import ACAException
from acacore.models.file import File


class ExtractError(ACAException):
    """Base class for unarchiver exceptions."""

    def __init__(self, file: File, msg: str = "Extraction error") -> None:
        self.file: File = file
        self.msg: str = msg
        super().__init__(msg)


class PasswordProtectedError(ExtractError):
    """Archive is encrypted."""


class UnrecognizedFileError(ExtractError):
    """Archive file cannot be opened or recognized."""


class NotPreservableFileError(ExtractError):
    """Archive file cannot be preserved."""


class ExtractorBase(ABC):
    tool_names: ClassVar[list[str]]

    def __init__(self, database: FileDB, file: File, root: Path | None = None) -> None:
        self.database: FileDB = database
        self.file: File = file
        self.file.root = root or self.file.root or database.path.parent.parent

    @property
    def extract_folder(self):
        path: Path = self.file.get_absolute_path().parent / f"_{self.file.uuid.hex}"
        while path.exists() and not path.is_dir():
            path = path.with_name("_" + path.name)
        return path

    @abstractmethod
    def extract(self) -> Generator[tuple[Path, Path], None, None]:
        """
        Extract files from archive.

        :return: A tuple containing the extracted file path and the original path before sanitization.
        """
        ...
