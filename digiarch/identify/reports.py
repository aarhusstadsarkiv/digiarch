"""Reporting utilities for file discovery.

"""

# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------
import pandas as pd
from pathlib import Path
from digiarch.data import get_fileinfo_list
from typing import List

# -----------------------------------------------------------------------------
# Function Definitions
# -----------------------------------------------------------------------------


def report_results(data_file: Path, save_path: Path) -> None:
    """Generates reports of explore_dir() results.

    Parameters
    ----------
    data_file: str
        Data file containing information from which to generate reports.
    save_path: str
        The path in which to save the reports.

    """
    # Type declarations
    report_file: Path
    files: List[dict] = []
    files_df: pd.DataFrame
    file_exts_count: pd.DataFrame

    # Get file information from data file
    info = get_fileinfo_list(data_file)

    # Collect file information
    files = [f.to_dict() for f in info]
    # empty_subs = [f.path for f in info if f.is_empty_sub is True]

    # We might get an empty directory
    if files:
        # Generate reports
        report_file = Path(save_path, "file_exts.csv")
        files_df = pd.DataFrame(data=files)
        # Count extensions
        file_exts_count = (
            files_df.groupby("ext").size().rename("count").to_frame()
        )
        file_exts_sorted = file_exts_count.sort_values(
            "count", ascending=False
        )
        file_exts_sorted.to_csv(report_file, header=True)
