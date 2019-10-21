"""Implements data classes used throughout the package.

"""

# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------
import dataclasses
import json
from typing import Optional

# -----------------------------------------------------------------------------
# Classes
# -----------------------------------------------------------------------------
@dataclasses.dataclass
class FileInfo:
    """Class for keeping track of file information"""

    name: str = ""
    ext: str = ""
    is_empty_sub: bool = False
    path: str = ""
    mime_type: str = ""
    guessed_ext: str = ""

    def to_dict(self) -> dict:
        return dataclasses.asdict(self)

    def to_json(self) -> str:
        return json.dumps(self, cls=FileInfoEncoder)


class FileInfoEncoder(json.JSONEncoder):
    def default(self, file_info: FileInfo) -> Optional[dict]:
        # Since type checking is not enforced at runtime
        if isinstance(file_info, FileInfo):
            return file_info.to_dict()
        return super().default(file_info)
