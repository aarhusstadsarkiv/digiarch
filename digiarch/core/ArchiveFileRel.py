# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------
import re
from pathlib import Path
from typing import Any
from typing import Dict
from typing import Optional
from typing import Pattern
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

    path: Path
    uuid: UUID4 = Field(None)
    checksum: Optional[str]
    # aars_path: Path = Field(None)
    relative_path: Path = Field(None)

    # Validators
    @validator("path")
    def path_must_be_file(cls, path: Path) -> Path:
        """Resolves the file path and validates that it points
        to an existing file."""

        if not path.resolve().is_file():
            raise ValueError("File does not exist")
        return path.resolve()

    @validator("uuid", pre=True, always=True)
    def set_uuid(cls, uuid: UUID4) -> UUID:
        return uuid or uuid4()

    @validator("relative_path", pre=True, always=True)
    def set_aars_path(cls, relative_path: Path, values: Dict[str, Any]) -> Path:
        path: Optional[Path] = values.get("path")
        if path:
            return path.relative_to(os.environ["ROOTPATH"])

    def read_text(self) -> str:
        """Expose read_text() functionality from pathlib.
        Encoding is set to UTF-8.

        Returns
        -------
        str
            File text data.
        """
        return self.path.read_text(encoding="utf-8")

    def read_bytes(self) -> bytes:
        """Expose read_bytes() functionality from pathlib.

        Returns
        -------
        bytes
            File byte data.
        """
        return self.path.read_bytes()

    def name(self) -> str:
        """Get the file name.

        Returns
        -------
        str
            File name.
        """
        return self.path.name

    def ext(self) -> str:
        """Get the file extension.

        Returns
        -------
        str
            File extension.
        """
        return self.path.suffix.lower()

    def size(self) -> str:
        """Get the file size in human readable string format.

        Returns
        -------
        str
            File size in human readable format.
        """
        return size_fmt(self.path.stat().st_size)


class ArchiveFile(Identification, File):
    """ArchiveFile data model."""
