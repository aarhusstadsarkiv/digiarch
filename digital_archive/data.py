"""Implements data classes and related utilities used throughout
Digital Archive.

"""

# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------
import dataclasses
import dacite
import json
import tqdm
from typing import Any, List

# -----------------------------------------------------------------------------
# Classes
# -----------------------------------------------------------------------------


@dataclasses.dataclass
class FileInfo:
    """Dataclass for keeping track of file information"""

    name: str = ""
    ext: str = ""
    is_empty_sub: bool = False
    path: str = ""
    mime_type: str = ""
    guessed_ext: str = ""

    def to_dict(self) -> dict:
        """Avoid having to import dataclasses all the time."""
        return dataclasses.asdict(self)

    def to_json(self) -> str:
        """Return json dump using
        :class:`~digital_archive.data.DataclassEncoder`"""
        return json.dumps(self, default=encode_dataclass)

    @staticmethod
    def from_dict(data: dict) -> Any:
        return dacite.from_dict(data_class=FileInfo, data=data)


# -----------------------------------------------------------------------------
# Function Definitions
# -----------------------------------------------------------------------------
def encode_dataclass(data_cls: object) -> dict:
    try:
        return dataclasses.asdict(data_cls)
    except TypeError:
        type_name = type(data_cls)
        raise TypeError(
            f"Object of type {type_name} is not serializable with this default"
        )


def load_json_list(data_file: str) -> List[dict]:
    with open(data_file) as file:
        data: List[dict] = json.load(file)
    return data


def get_fileinfo_list(data_file: str) -> List[FileInfo]:
    # Read file info from data file
    data: List[dict] = tqdm.tqdm(
        load_json_list(data_file), desc="Reading file information"
    )
    # Load file info into list
    info: List[FileInfo] = tqdm.tqdm(
        [FileInfo.from_dict(d) for d in data], desc="Loading file information"
    )
    return info
