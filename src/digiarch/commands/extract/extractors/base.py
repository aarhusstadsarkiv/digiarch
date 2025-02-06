from abc import ABC
from abc import abstractmethod
from pathlib import Path
from typing import ClassVar

from acacore.exceptions.base import AcacoreError
from acacore.models.file import BaseFile


class ExtractError(AcacoreError):
    """Base class for unarchiver exceptions."""

    def __init__(self, file: BaseFile, msg: str = "Extraction error", *args: object) -> None:
        self.file: BaseFile = file
        self.msg: str = msg
        super().__init__(msg, *args)


class PasswordProtectedError(ExtractError):
    """Archive is encrypted."""


class UnrecognizedFileError(ExtractError):
    """Archive file cannot be opened or recognized."""


class NotPreservableFileError(ExtractError):
    """Archive file cannot be preserved."""


class ExtractorBase(ABC):
    tool_names: ClassVar[list[str]]

    def __init__(self, file: BaseFile, root: Path | None = None) -> None:
        self.file: BaseFile = file
        self.file.root = root or self.file.root

    @property
    def extract_folder(self):
        path: Path = self.file.get_absolute_path().parent / f"_{self.file.uuid.hex}"
        while path.exists() and not path.is_dir():
            path = path.with_name("_" + path.name)
        return path

    @abstractmethod
    def extract(self) -> list[tuple[Path, Path]]:
        """
        Extract files from archive.

        :return: A list of tuples containing the extracted file path and the original path before sanitization.
        """
        ...
