"""Reporting utilities for file discovery.

"""

# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------
import pandas as pd
from pathlib import Path
from typing import List, Dict, Optional, Any
from digiarch.internals import FileInfo, to_json

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
    file_dicts: List[Dict[Any, Any]] = [f.to_dict() for f in files]

    # We might get an empty directory
    if file_dicts:
        # Create new folder in save path
        save_path: Path = save_path / "reports"
        save_path.mkdir(exist_ok=True)

        # Generate data frame
        files_df = pd.DataFrame(data=file_dicts)

        # Count extensions
        file_exts_count = (
            files_df.groupby("ext").size().rename("count").to_frame()
        )
        file_exts_sorted = file_exts_count.sort_values(
            "count", ascending=False
        )

        # Find identification warnings
        file_id_warnings = files_df[files_df.identification.notnull()]
        id_warnings: List[Dict[str, Dict[str, Any]]] = dict()
        for _, row in file_id_warnings.iterrows():
            if row["identification"].get("warning") is not None:
                warn_dict = {
                    "identification": row["identification"],
                    "name": row["name"],
                }
                id_warnings.update({str(row["path"]): warn_dict})

        # Save reports
        file_exts_sorted.to_csv(save_path / "file_extensions.csv", header=True)
        if id_warnings:
            to_json(id_warnings, save_path / "identification_warnings.json")
