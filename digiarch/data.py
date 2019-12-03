"""Implements data classes and related utilities used throughout
Digital Archive.

"""

# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------
import dataclasses
import dacite
import json
from typing import Any, List

# -----------------------------------------------------------------------------
# Classes
# -----------------------------------------------------------------------------

# File Info
# --------------------


@dataclasses.dataclass
class FileInfo:
    """Dataclass for keeping track of file information"""

    name: str = ""
    ext: str = ""
    is_empty_sub: bool = False
    path: str = ""
    checksum: str = ""

    def to_dict(self) -> dict:
        """Avoid having to import dataclasses all the time."""
        return dataclasses.asdict(self)

    def replace(self, **fields):
        return dataclasses.replace(self, **fields)

    @staticmethod
    def from_dict(data: dict) -> Any:
        return dacite.from_dict(data_class=FileInfo, data=data)


# Utility
# --------------------


class DataJSONEncoder(json.JSONEncoder):
    """DataJSONEncoder subclasses JSONEncoder in order to handle
    encoding of dataclasses."""

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
        return super().default(obj)


# -----------------------------------------------------------------------------
# Function Definitions
# -----------------------------------------------------------------------------


def dump_file(data: object, file: str) -> None:
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

    with open(file, "w+") as f:
        json.dump(data, f, indent=4, cls=DataJSONEncoder, ensure_ascii=False)


def load_json_list(data_file: str) -> List[dict]:
    with open(data_file) as file:
        data: List[dict] = json.load(file)
    return data


def get_fileinfo_list(data_file: str) -> List[FileInfo]:
    # Read file info from data file
    data: List[dict] = load_json_list(data_file)

    # Load file info into list
    info: List[FileInfo] = [FileInfo.from_dict(d) for d in data]
    return info
