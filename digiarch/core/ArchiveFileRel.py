# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------


from pathlib import Path
from typing import Any
from typing import Optional
from uuid import UUID
from uuid import uuid4

from acamodels._internals import size_fmt
from acamodels.aca_base import ACABase
from acamodels.identification import Identification
from pydantic import Field
from pydantic import UUID4
from pydantic import validator
import os

# -----------------------------------------------------------------------------
# Model
# -----------------------------------------------------------------------------


class File(ACABase):
    """File data model"""

    uuid: UUID4 = Field(None)
    checksum: Optional[str]
    relative_path: Path = Field(None)
    is_binary: bool = Field(None)

    # Validators
    @validator("relative_path")
    def path_must_be_file(cls, path: Path) -> Path:
        """Resolves the file path and validates that it points
        to an existing file."""
        absolute_path = Path(os.environ["ROOTPATH"], path)
        if not absolute_path.resolve().is_file():
            raise ValueError("File does not exist")
        return path

    @validator("uuid", pre=True, always=True)
    def set_uuid(cls, uuid: UUID4) -> UUID:
        return uuid or uuid4()

    def read_text(self) -> Any:
        """Expose read_text() functionality from pathlib.
        Encoding is set to UTF-8.

        Returns
        -------
        str
            File text data.
        """
        return self.relative_path.read_text(encoding="utf-8")

    def read_bytes(self) -> bytes:
        """Expose read_bytes() functionality from pathlib.

        Returns
        -------
        bytes
            File byte data.
        """

        absolute_path: Path = Path(os.environ["ROOTPATH"], self.relative_path)
        return absolute_path.read_bytes()

    def name(self) -> Any:
        """Get the file name.

        Returns
        -------
        str
            File name.
        """
        return self.relative_path.name

    def ext(self) -> Any:
        """Get the file extension.

        Returns
        -------
        str
            File extension.
        """
        return self.relative_path.suffix.lower()

    def size(self) -> Any:
        """Get the file size in human readable string format.

        Returns
        -------
        str
            File size in human readable format.
        """
        return size_fmt(self.relative_path.stat().st_size)


class ArchiveFile(Identification, File):
    """ArchiveFile data model."""
