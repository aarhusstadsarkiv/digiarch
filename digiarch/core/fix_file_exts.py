"""Module level docstring.

"""

# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------

import json
from pathlib import Path
from typing import List


from digiarch.core.ArchiveFileRel import ArchiveFile
from tqdm import tqdm

# -----------------------------------------------------------------------------
# Function Definitions
# -----------------------------------------------------------------------------


def fix_extensions(files: List[ArchiveFile]) -> List[ArchiveFile]:
    map_path = Path(__file__).parents[1] / "_data" / "ext_map.json"
    ext_map = json.load(map_path.open(encoding="utf-8"))
    to_fix = [
        file
        for file in files
        if "Extension mismatch" in (file.warning or "")
        and file.puid in ext_map
    ]
    for file in tqdm(
        to_fix, desc="Fixing file extensions", disable=not to_fix
    ):
        new_name = file.relative_path.with_name(
            f"{file.name()}.{ext_map[file.puid]}"
        )
        file.relative_path.rename(new_name)
        file.relative_path = new_name

    return to_fix
