"""Module for finding and fixing files in archive hand-ins.

Functions
---------
explore_dir(path)
    Finds files and empty directories in the given path.
report_results(file_exts, empty_dirs, save_path)
    Reports the results of explore_dir to a file in `save_path`

"""
# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------
import os
import pandas as pd
import argparse
from typing import Tuple, List

# -----------------------------------------------------------------------------
# Function Definitions
# -----------------------------------------------------------------------------


def dir_path(input: str) -> str:
    """Checks if a given string is a valid path.

    Parameters
    ----------
    input : str
        The input to check.

    Returns
    -------
    input : str
        The input string is returned if it is a valid directory.

    Raises
    ------
    NotADirectoryError
        Input string was not a directory.

    """
    if os.path.isdir(input):
        return input
    else:
        raise NotADirectoryError(input)


def create_parser() -> argparse.ArgumentParser:
    """Spawns a parser using argparse for input arguments.

    Returns
    -------
    parser : argparse.ArgumentParser
        Parser with arguments created using argparse

    """
    parser = argparse.ArgumentParser(
        description="Output an overview of file extensions."
    )
    parser.add_argument("path", type=dir_path, help="Path to search.")
    return parser


def explore_dir(path: str) -> Tuple[list, list]:
    """Finds files and empty directories in the given path.

    Parameters
    ----------
    path : str
        The path in which to find files

    Returns
    -------
    file_exts : list
        Two-dimensional array of file extensions and their root directories.
    empty_dirs : list
        List of empty directories found in the search.

    """
    empty_dirs: List[str] = []
    file_exts: List[List[str]] = []

    if not os.listdir(path):
        # Function was called on empty directory
        # Return empty lists
        return file_exts, empty_dirs

    for root, dirs, files in os.walk(path):
        for f in files:
            ext = os.path.splitext(f)[1]
            file_exts.append([ext.lower(), root])
        if not dirs and not files:
            empty_dirs.append(root)

    return file_exts, empty_dirs


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


def main(args) -> None:
    file_exts, empty_dirs = explore_dir(args.path)
    report_results(file_exts, empty_dirs, args.path)


if __name__ == "__main__":
    try:
        parser = create_parser()
        args = parser.parse_args()
    except NotADirectoryError as not_dir:
        print(f"{not_dir} is not a directory!")

    main(args)
