"""Implements data classes and related utilities used throughout
Digital Archive.

"""

# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------

import dataclasses
import inspect
import json
from datetime import datetime
from pathlib import Path
from typing import Any, List, Optional, Set

import dacite
from dateutil.parser import parse as date_parse

import digiarch
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

# Base class
# --------------------


@dataclasses.dataclass
class DataBase:
    def to_dict(self) -> dict:
        return dataclasses.asdict(self)

    def replace(self, **fields: Any) -> Any:
        return dataclasses.replace(self, **fields)

    @classmethod
    def from_dict(cls, data: dict) -> Any:
        # For some reason the post init yields str for FileData
        if isinstance(data.get("digiarch_dir"), str):
            data.update({"digiarch_dir": Path(data.get("digiarch_dir", ""))})
        if isinstance(data.get("json_file"), str):
            data.update({"json_file": Path(data.get("json_file", ""))})
        return dacite.from_dict(
            data_class=cls, data=data, config=dacite.Config(check_types=False)
        )


# Identification
# --------------------


@dataclasses.dataclass
class Identification(DataBase):
    """Data class for keeping track of file identification information"""

    puid: Optional[str]
    signame: Optional[str]
    warning: Optional[str] = None


# File Info
# --------------------


@dataclasses.dataclass
class FileInfo(DataBase):
    """Data class for keeping track of file information"""

    path: Path
    name: str = dataclasses.field(init=False)
    ext: str = dataclasses.field(init=False)
    size: str = dataclasses.field(init=False)
    checksum: Optional[str] = None
    identification: Optional[Identification] = None

    def __post_init__(self) -> None:
        # JSON/from_dict compatibility
        if not isinstance(self.path, Path):
            self.path = Path(self.path)

        # Resolve path, init fields
        self.path = self.path.resolve()
        self.name = self.path.name
        self.ext = self.path.suffix.lower()
        self.size = size_fmt(self.path.stat().st_size)


# Metadata
# --------------------


@dataclasses.dataclass
class Metadata(DataBase):
    """Data class for keeping track of metadata used in data.json"""

    last_run: datetime
    processed_dir: Path
    file_count: Optional[int] = None
    total_size: Optional[str] = None
    duplicates: Optional[int] = None
    identification_warnings: Optional[int] = None
    empty_subdirs: Optional[List[Path]] = None
    several_files: Optional[List[Path]] = None

    def __post_init__(self) -> None:
        # JSON/from_dict compatibility
        if isinstance(self.processed_dir, str):
            self.processed_dir = Path(self.processed_dir)
        if isinstance(self.last_run, str):
            self.last_run = date_parse(self.last_run)

        # Resolve path
        self.processed_dir = self.processed_dir.resolve()


# JSON Data
# --------------------
@dataclasses.dataclass
class FileData(DataBase):
    """Data class collecting Metadata and list of FileInfo"""

    metadata: Metadata
    files: List[FileInfo] = dataclasses.field(default_factory=list)
    digiarch_dir: Path = dataclasses.field(init=False)
    json_file: Path = dataclasses.field(init=False)

    def __post_init__(self) -> None:
        # Directory paths
        self.digiarch_dir = Path(self.metadata.processed_dir, "_digiarch")
        data_dir = self.digiarch_dir / ".data"
        self.json_file = data_dir / "data.json"

        # Create directories
        self.digiarch_dir.mkdir(exist_ok=True)
        data_dir.mkdir(exist_ok=True)

        # Create data file if it does not exist
        if not self.json_file.is_file():
            self.json_file.touch()

    def to_json(self, file: Optional[Path] = None) -> None:
        if file is None:
            file = self.json_file
        with file.open("w", encoding="utf-8") as f:
            json.dump(
                self, f, indent=4, cls=DataJSONEncoder, ensure_ascii=False
            )

    @classmethod
    def from_json(cls, data_file: Path) -> Any:
        with data_file.open("r", encoding="utf-8") as file:
            return cls.from_dict(json.load(file))


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
        if dataclasses.is_dataclass(obj):
            return dataclasses.asdict(obj)
        if isinstance(obj, Path):
            return str(obj)
        if isinstance(obj, datetime):
            return obj.isoformat()
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


def natsort_path(file_list: List[FileInfo]) -> List[FileInfo]:
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

    sorted_file_list: List[FileInfo] = natsorted(
        file_list, key=lambda fileinfo: str(fileinfo.path)
    )

    return sorted_file_list
