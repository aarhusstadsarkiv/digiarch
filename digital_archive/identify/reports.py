"""Reporting utilities for file discovery.

"""

# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------
import os
import tqdm
import pandas as pd
from digital_archive.data import load_json_list, FileInfo
from digital_archive.utils import click_utils
from typing import List

# -----------------------------------------------------------------------------
# Function Definitions
# -----------------------------------------------------------------------------


def report_results(data_file: str, save_path: str) -> None:
    """Generates reports of explore_dir() results.

    Parameters
    ----------
    data_file: str
        Data file containing information from which to generate reports.
    save_path: str
        The path in which to save the reports.

    Returns
    -------
    None

    """
    # Type declarations
    report_file: str = ""
    empty_subs_file: str = ""
    files: List[dict] = []
    empty_subs: List[str] = []
    files_df: pd.DataFrame
    files_df_count: pd.DataFrame

    # Load file info
    data: List[dict] = tqdm.tqdm(
        load_json_list(data_file), desc="Reading file information"
    )
    info: List[FileInfo] = tqdm.tqdm(
        [FileInfo.from_dict(d) for d in data], desc="Loading file information"
    )

    # # print(json.loads(data_file))

    # # Collect file information
    files = [f.to_dict() for f in info if f.is_empty_sub is False]
    empty_subs = [f.path for f in info if f.is_empty_sub is True]

    # We might get an empty directory
    if files:
        # Generate reports
        report_file = os.path.join(save_path, "file_exts.csv")
        files_df = pd.DataFrame(data=files)
        # Count extensions
        file_exts_count = (
            files_df.groupby("ext").size().rename("count").to_frame()
        )
        file_exts_count.to_csv(report_file, header=True)
        click_utils.click_ok(f"Wrote file extension report to {report_file}")

    # Generate separate report if there are empty subdirectories
    if empty_subs:
        empty_subs_file = os.path.join(save_path, "empty_subs.txt")
        with open(empty_subs_file, "w+") as f:
            for sub in empty_subs:
                f.write(sub + "\n")
        click_utils.click_warn("There are empty subdirectories!")
        click_utils.click_warn(
            f"Consult {empty_subs_file} for more information."
        )
