"""Implements data classes and related utilities used throughout
Digital Archive.

"""

# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------
import dataclasses
import json
from typing import Any

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
        return json.dumps(self, cls=DataclassEncoder)


class DataclassEncoder(json.JSONEncoder):
    """JSONEncoder subclass supporting dataclass serialization."""

    def default(self, obj: object) -> Any:
        if dataclasses.is_dataclass(obj):
            return dataclasses.asdict(obj)
        return super().default(obj)
