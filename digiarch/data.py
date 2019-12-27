"""Implements data classes and related utilities used throughout
Digital Archive.

"""

# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------
import dataclasses
import json
import dacite
from pathlib import Path
from typing import Any, List, Optional

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
        if data.get("path"):
            data.update({"path": Path(str(data.get("path")))})
        return dacite.from_dict(data_class=cls, data=data)


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
    checksum: Optional[str] = None
    identification: Optional[Identification] = None


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


def load_json_list(data_file: Path) -> List[dict]:
    with data_file.open("r", encoding="utf-8") as file:
        data: List[dict] = json.load(file)
    return data


def get_fileinfo_list(data_file: Path) -> List[FileInfo]:
    # Read file info from data file
    data: List[dict] = load_json_list(data_file)

    # Load file info into list
    info: List[FileInfo] = [FileInfo.from_dict(d) for d in data]
    return info
