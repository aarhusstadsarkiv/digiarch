"""Reporting utilities for file discovery.

"""


# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------

from collections import Counter
from pathlib import Path
from typing import Dict, List

from acamodels import Identification
from digiarch.internals import ArchiveFile, to_json
from tqdm import tqdm

# -----------------------------------------------------------------------------
# Function Definitions
# -----------------------------------------------------------------------------


def report_results(files: List[ArchiveFile], save_path: Path) -> None:
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
    id_warnings: Dict[str, List[Dict[str, Identification]]] = dict()
    warning_key: str
    warning_count: Counter = Counter()

    # Collect information
    for file in tqdm(files, desc="Creating reports"):
        ext_count.update([file.ext()])
        if file.identification and file.identification.warning:
            if "No match" in file.identification.warning:
                warning_key = "No match"
            else:
                warning_key = file.identification.warning
            warning_list = id_warnings.get(warning_key, [])
            warning_list.append({str(file.path): file.identification.json()})
            id_warnings.update({warning_key: warning_list})

    for warning_key, warning_list in id_warnings.items():
        warning_count.update({warning_key: len(warning_list)})

    file_exts: Dict[str, int] = dict(ext_count.most_common())
    identification_warnings = {
        "counts": dict(warning_count),
        "warnings": id_warnings,
    }

    if files:
        # Create new folder in save path
        save_path = save_path / "reports"
        save_path.mkdir(exist_ok=True)

        # Save files
        to_json(file_exts, save_path / "file_extensions.json")
        to_json(
            identification_warnings,
            save_path / "identification_warnings.json",
        )
