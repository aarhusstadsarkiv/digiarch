"""Implements data classes used throughout the package.

"""

# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------
from dataclasses import dataclass, asdict

# -----------------------------------------------------------------------------
# Classes
# -----------------------------------------------------------------------------
@dataclass
class FileInfo:
    """Class for keeping track of file information"""

    name: str = ""
    ext: str = ""
    is_empty_sub: bool = False
    path: str = ""
    mime_type: str = ""
    guessed_ext: str = ""

    def to_dict(self) -> dict:
        return asdict(self)
