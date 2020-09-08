"""Module level docstring.

"""

# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------

from pathlib import Path
from typing import List, Set

from digiarch.internals import IGNORED_EXTS, ArchiveFile
from tqdm import tqdm

# -----------------------------------------------------------------------------
# Function Definitions
# -----------------------------------------------------------------------------


def grouping(files: List[ArchiveFile], save_path: Path) -> None:
    """Groups files per file extension.

    Parameters
    ----------
    data_file : str
        File from which data is read.
    save_path : str
        Path to save results to
    """

    # Initialise variables
    ignored_group: List[str] = []
    exts: Set[str] = {file.ext() for file in files}

    # Create new folder in save path
    save_path = save_path / "grouped_files"
    save_path.mkdir(exist_ok=True)

    # Group files per file extension.
    for ext in tqdm(exts, desc="Grouping files"):
        group_out: Path = save_path / f"{ext}_files.txt"
        file_group: List[str] = []
        for file in files:
            if file.ext() in IGNORED_EXTS:
                ignored_group.append(str(file.path))
            if file.ext() == ext:
                file_group.append(str(file.path))
        group_out.write_text("\n".join(file_group), encoding="utf-8")

    # Save ignored files
    ignored_out: Path = save_path / "ignored_files.txt"
    ignored_out.write_text("\n".join(ignored_group), encoding="utf-8")
