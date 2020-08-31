"""Module level docstring.

"""

# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------

import json
from pathlib import Path
from typing import List

from tqdm import tqdm

from digiarch.internals import ArchiveFile

# -----------------------------------------------------------------------------
# Function Definitions
# -----------------------------------------------------------------------------


def fix_extensions(files: List[ArchiveFile]) -> None:
    map_path = Path(__file__).parents[1] / "_data" / "ext_map.json"
    ext_map = json.load(map_path.open(encoding="utf-8"))
    for file in tqdm(files, desc="Fixing file extensions"):
        # if file.identification:
        warning = file.warning or ""
        puid = file.puid
        if "Extension mismatch" in warning and puid in ext_map:
            new_name = file.path.with_name(f"{file.name()}.{ext_map[puid]}")
            file.path.rename(new_name)
