"""Module level docstring.

"""

# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------
from pathlib import Path
from typing import List, Set
from digiarch.data import get_fileinfo_list, FileInfo

# -----------------------------------------------------------------------------
# Function Definitions
# -----------------------------------------------------------------------------


def grouping(data_file: Path, save_path: Path) -> None:
    """Groups files per file extension.

    Parameters
    ----------
    data_file : str
        File from which data is read.
    save_path : str
        Path to save results to
    """

    # Initialise variables
    files: List[FileInfo] = get_fileinfo_list(data_file)
    exts: Set[str] = {file.ext for file in files}

    # Group files per file extension.
    for ext in exts:
        ext_file = Path(save_path, f"{ext}_files.txt")
        with ext_file.open("w", encoding="utf-8") as out_file:
            for file in files:
                if file.ext == ext:
                    out_file.write(f"{file.path}\n")
