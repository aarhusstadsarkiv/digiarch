"""Reporting utilities for file discovery.

"""

# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------
import pandas as pd
from pathlib import Path
from digiarch.data import FileInfo
from typing import List

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
    # Type declarations
    report_file: Path
    files_df: pd.DataFrame
    file_exts_count: pd.DataFrame

    # Collect file information
    file_dicts: List[dict] = [f.to_dict() for f in files]

    # We might get an empty directory
    if file_dicts:
        # Generate reports
        report_file = Path(save_path, "file_exts.csv")
        files_df = pd.DataFrame(data=file_dicts)
        # Count extensions
        file_exts_count = (
            files_df.groupby("ext").size().rename("count").to_frame()
        )
        file_exts_sorted = file_exts_count.sort_values(
            "count", ascending=False
        )
        file_exts_sorted.to_csv(report_file, header=True)
