"""Module level docstring.

"""

# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------

import json
from pathlib import Path
from typing import List

from tqdm import tqdm
from acamodels import ArchiveFile

# -----------------------------------------------------------------------------
# Function Definitions
# -----------------------------------------------------------------------------


def fix_extensions(files: List[ArchiveFile]) -> bool:
    fixed: bool
    map_path = Path(__file__).parents[1] / "_data" / "ext_map.json"
    ext_map = json.load(map_path.open(encoding="utf-8"))
    to_fix = [
        file
        for file in files
        if "Extension mismatch" in (file.warning or "")
        and file.puid in ext_map
    ]
    for file in tqdm(to_fix, desc="Fixing file extensions"):
        new_name = file.path.with_name(f"{file.name()}.{ext_map[file.puid]}")
        file.path.rename(new_name)
    return bool(to_fix)
