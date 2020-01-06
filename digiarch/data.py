"""Implements data classes and related utilities used throughout
Digital Archive.

"""

# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------
import dataclasses
import inspect
import json
import dacite
from datetime import datetime
from dateutil.parser import parse as date_parse
from pathlib import Path
from typing import Any, List, Optional, Set
import digiarch

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

    name: str
    ext: str
    path: Path
    size: str
    checksum: Optional[str] = None
    identification: Optional[Identification] = None

    # JSON/from_dict compatibility
    def __post_init__(self) -> None:
        if isinstance(self.path, str):
            self.path = Path(self.path)


# Metadata
# --------------------


@dataclasses.dataclass
class Metadata(DataBase):
    """Data class for keeping track of metadata used in data.json"""

    created_on: datetime
    file_count: int
    total_size: str
    processed_dir: Path
    duplicates: Optional[int] = None
    identification_warnings: Optional[int] = None
    empty_subdirectories: Optional[List[Path]] = None

    # JSON/from_dict compatibility
    def __post_init__(self) -> None:
        if isinstance(self.processed_dir, str):
            self.processed_dir = Path(self.processed_dir)
        if isinstance(self.created_on, str):
            self.created_on = date_parse(self.created_on)


# JSON Data
# --------------------
@dataclasses.dataclass
class FileData(DataBase):
    """Data class collecting FileInfo lists and Metadata"""

    metadata: Metadata
    files: List[FileInfo]


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


def dump_file(data: object, file: Path) -> None:
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


def get_fileinfo_list(data_file: Path) -> List[FileInfo]:
    # Read file info from data file
    with data_file.open("r", encoding="utf-8") as file:
        data: List[dict] = json.load(file).get("files", [{}])

    # Load file info into list
    info: List[FileInfo] = [FileInfo.from_dict(d) for d in data]
    return info


def get_data(data_file: Path) -> Any:
    with data_file.open("r", encoding="utf-8") as file:
        return FileData.from_dict(json.load(file))
