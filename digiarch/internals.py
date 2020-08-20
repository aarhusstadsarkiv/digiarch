"""Implements data classes and related utilities used throughout
Digital Archive.

"""

# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------

import inspect
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from pydantic import Field, root_validator, validator

import digiarch
from acamodels import ACABase, ArchiveFile
from natsort import natsorted

# -----------------------------------------------------------------------------
# Globals
# -----------------------------------------------------------------------------

IGNORED_EXTS: Set[str] = json.load(
    (
        Path(inspect.getfile(digiarch)).parent / "_data" / "blacklist.json"
    ).open()
).keys()

# -----------------------------------------------------------------------------
# Classes
# -----------------------------------------------------------------------------


# Metadata
# --------------------


class Metadata(ACABase):
    """Data class for keeping track of metadata used in data.json"""

    last_run: datetime
    processed_dir: Path
    file_count: Optional[int] = None
    total_size: Optional[str] = None
    duplicates: Optional[int] = None
    identification_warnings: Optional[int] = None
    empty_subdirs: Optional[List[Path]] = None
    several_files: Optional[List[Path]] = None


# File Data
# --------------------


class FileData(ACABase):
    """Data class collecting Metadata and list of FileInfo"""

    metadata: Metadata
    files: List[ArchiveFile] = []
    digiarch_dir: Path = Field(None)
    json_file: Path = Field(None)

    @root_validator
    def create_directories(cls, fields: Dict[Any, Any]) -> Dict[Any, Any]:
        metadata = fields.get("metadata")
        digiarch_dir = fields.get("digiarch_dir")
        json_file = fields.get("json_file")
        if digiarch_dir is None and metadata:
            digiarch_dir = metadata.processed_dir / "_digiarch"
            digiarch_dir.mkdir(exist_ok=True)
            fields["digiarch_dir"] = digiarch_dir
        if json_file is None and digiarch_dir:
            data_dir = digiarch_dir / ".data"
            data_dir.mkdir(exist_ok=True)
            json_file = data_dir / "data.json"
            json_file.touch()
            fields["json_file"] = json_file
        return fields

    @validator("digiarch_dir")
    def check_digiarch_dir(cls, digiarch_dir: Path) -> Path:
        if not digiarch_dir.is_dir():
            raise ValueError("Invalid digiarch directory path")
        return digiarch_dir

    @validator("json_file")
    def check_json_file(cls, json_file: Path) -> Path:
        if not json_file.is_file():
            raise ValueError("Invalid JSON file path")
        return json_file

    def dump(self) -> None:
        data = super().json(indent=2, ensure_ascii=False)
        self.json_file.write_text(data, encoding="utf-8")


# Utility
# --------------------


class DataJSONEncoder(json.JSONEncoder):
    """DataJSONEncoder subclasses JSONEncoder in order to handle
    encoding of data classes."""

    # pylint does not like this subclassing, even though it's the recommended
    # method. So we disable the warnings.

    # pylint: disable=method-hidden,arguments-differ
    def default(self, obj: object) -> Any:
        """Overrides the JSONEncoder default.

        Parameters
        ----------
        obj : object
            Object to encode.
        Returns
        -------
        dataclasses.asdict(obj) : dict
            If the object given is a data class, return it as a dict.
        super().default(obj) : Any
            If the object is not a data class, use JSONEncoder's default and
            let it handle any exception that might occur.
        """
        if isinstance(obj, Path):
            return str(obj)
        return super().default(obj)


# pylint: enable=method-hidden,arguments-differ


# -----------------------------------------------------------------------------
# Function Definitions
# -----------------------------------------------------------------------------


def size_fmt(size: float) -> str:
    """Formats a file size in binary multiples to a human readable string.

    Parameters
    ----------
    size : float
        The file size in bytes.

    Returns
    -------
    str
        Human readable string representing size in binary multiples.
    """
    for unit in ["B", "KiB", "MiB", "GiB", "TiB"]:
        if size < 1024.0:
            break
        size /= 1024.0
    return f"{size:.1f} {unit}"


def to_json(data: object, file: Path) -> None:
    """Dumps JSON files given data and a file path
    using :class:`~digiarch.data.DataJSONEncoder` as encoder.
    Output uses indent = 4 to get pretty and readable files.
    `ensure_ascii` is set to `False` so we can get our beautiful Danish
    letters in the output.

    Parameters
    ----------
    data : object
        The data to dump to the JSON file.
    dump_file: str
        Path to the file in which to dump JSON data.
    """

    with file.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, cls=DataJSONEncoder, ensure_ascii=False)


def natsort_path(file_list: List[ArchiveFile]) -> List[ArchiveFile]:
    """Naturally sort a list of FileInfo objects by their paths.

    Parameters
    ----------
    file_list : List[FileInfo]
        The list of FileInfo objects to be sorted.

    Returns
    -------
    List[FileInfo]
        The list of FileInfo objects naturally sorted by their path.
    """

    sorted_file_list: List[ArchiveFile] = natsorted(
        file_list, key=lambda archive_file: str(archive_file.path)
    )

    return sorted_file_list
