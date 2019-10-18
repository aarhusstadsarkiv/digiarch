"""Reporting utilities for file discovery.

"""

# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------
import os
import pandas as pd
from digital_archive.data import FileInfo
from typing import List, Tuple

# -----------------------------------------------------------------------------
# Function Definitions
# -----------------------------------------------------------------------------


def report_results(info: List[FileInfo], save_path: str) -> Tuple[str, str]:
    """Generates reports of explore_dir() results.

    Parameters
    ----------
    info: List[FileInfo]
        Information from which to generate reports.
    save_path: str
        The path in which to save the reports.

    Returns
    -------
    report_file: str
        The file in which the report was saved.
    empty_subs_file : str
        The file in which the empty subdirectory report was saved.

    """
    # Type declarations
    report_file: str = ""
    empty_subs_file: str = ""
    files: List[dict]
    empty_subs: List[str]
    files_df: pd.DataFrame
    files_df_count: pd.DataFrame

    # Collect file information
    files = [f.to_dict() for f in info if f.is_empty_sub is False]
    empty_subs = [f.path for f in info if f.is_empty_sub is True]

    # Generate reports
    report_file = os.path.join(save_path, "file_exts.csv")
    files_df = pd.DataFrame(data=files)
    # Count extensions
    file_exts_count = files_df.groupby("ext").size().rename("count").to_frame()
    file_exts_count.to_csv(report_file, header=True)

    # Generate separate report if there are empty subdirectories
    if empty_subs:
        empty_subs_file = os.path.join(save_path, "empty_subs.txt")
        with open(empty_subs_file, "w+") as f:
            for sub in empty_subs:
                f.write(sub + "\n")

    return report_file, empty_subs_file
