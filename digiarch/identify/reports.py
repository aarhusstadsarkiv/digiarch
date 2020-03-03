"""Reporting utilities for file discovery.

"""

# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------
from collections import Counter
from pathlib import Path
from typing import List, Dict
from digiarch.internals import FileInfo, Identification, to_json

# -----------------------------------------------------------------------------
# Function Definitions
# -----------------------------------------------------------------------------


def report_results(files: List[FileInfo], save_path: Path) -> None:
    """Generates reports of explore_dir() results.

    Parameters
    ----------
    files: List[FileInfo]
        The files to report on.
    save_path: str
        The path in which to save the reports.

    """

    # Initialise counters & dicts
    ext_count: Counter = Counter()
    id_warnings: Dict[str, Identification] = dict()

    # Collect information
    for file in files:
        ext_count.update([file.ext])
        if file.identification and file.identification.warning:
            id_warnings.update({str(file.path): file.identification})
    file_exts: Dict[str, int] = dict(ext_count.most_common())

    if files:
        # Create new folder in save path
        save_path = save_path / "reports"
        save_path.mkdir(exist_ok=True)

        # Save files
        to_json(file_exts, save_path / "file_extensions.json")
        if id_warnings:
            to_json(id_warnings, save_path / "identification_warnings.json")
