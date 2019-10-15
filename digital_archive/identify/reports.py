"""Reporting utilities for file discovery.

"""

# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------
import os
import pandas as pd

# -----------------------------------------------------------------------------
# Function Definitions
# -----------------------------------------------------------------------------


def report_results(file_exts: list, empty_dirs: list, save_path: str) -> None:
    """Generates reports of explore_dir() results.

    Parameters
    ----------
    file_exts : list
        Two-dimensional array with file extensions and root paths.
    empty_dirs : list
        List of empty directories found in a search.
    save_path: str
        The path in which to save the reports.

    Returns
    -------
    None

    """
    if file_exts:
        save_file = os.path.join(save_path, "file_exts.csv")
        file_exts_df = pd.DataFrame(
            data=file_exts, columns=["FileExt", "Root"]
        )
        file_exts_group = file_exts_df.groupby(
            "FileExt", as_index=False
        ).count()
        file_exts_group = file_exts_group.rename(columns={"Root": "Count"})
        file_exts_group.to_csv(
            save_file, header=True, index=False, encoding="utf-8"
        )
        print(f"Wrote file ext report to {save_file}")

    if empty_dirs:
        save_file = os.path.join(save_path, "empty_dirs.txt")
        with open(save_file, "w+") as f:
            for dir in empty_dirs:
                f.write(dir + "\n")
        print("There are empty directories!")
        print(f"Consult {save_file} for more information")

    if not file_exts and not empty_dirs:
        print("Base directory is empty. No reports produced.")
