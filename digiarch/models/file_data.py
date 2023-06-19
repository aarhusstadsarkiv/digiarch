# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------
from pathlib import Path
from typing import Any

from acamodels import ACABase
from pydantic import Field, root_validator

from digiarch.core.ArchiveFileRel import ArchiveFile
from digiarch.database import FileDB

# -----------------------------------------------------------------------------
# Model
# -----------------------------------------------------------------------------


class FileData(ACABase):
    main_dir: Path
    data_dir: Path = Field(None)
    db: FileDB = Field(None)
    files: list[ArchiveFile]

    class Config:
        arbitrary_types_allowed = True

    @root_validator
    def create_dir(cls, fields: dict[Any, Any]) -> dict[Any, Any]:
        main_dir = fields.get("main_dir")
        data_dir = fields.get("data_dir")
        db = fields.get("db")
        if data_dir is None and main_dir:
            data_dir = main_dir / "_metadata"
            data_dir.mkdir(exist_ok=True)
            fields["data_dir"] = data_dir
        if db is None and data_dir:
            db_path: Path = fields["data_dir"] / "files.db"
            fields["db"] = FileDB(f"sqlite:///{db_path}")
        return fields
