"""Module for finding and fixing files in archive hand-ins.

Functions
---------
explore_dir(path)
    Finds files and empty directories in the given path.
report_results(file_exts, empty_dirs, save_path)

"""
# -----------------------------------------------------------------------------
# Imports & Setup
# -----------------------------------------------------------------------------
# Imports
import os
import pandas as pd
import argparse


def dir_path(str):
    if os.path.isdir(str):
        return str
    else:
        raise NotADirectoryError(str)


def create_parser():
    parser = argparse.ArgumentParser(
        description="Output an overview of file extensions."
    )
    parser.add_argument("path", type=dir_path, help="Path to search.")
    return parser


def explore_dir(path):
    """Finds files and empty directories in the given path.

    Parameters
    ----------
    path
        The path in which to find files

    Returns
    -------
    file_exts
        List of file extensions found in the search.
    empty_dirs
        List of empty directories found in the search.

    """
    empty_dirs = []
    file_exts = []
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


def report_results(file_exts, empty_dirs, save_path):
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
        print("Wrote file ext report to {}".format(save_file))
    if empty_dirs:
        save_file = os.path.join(save_path, "empty_dirs.txt")
        with open(save_file, "w+") as f:
            for dir in empty_dirs:
                f.write(dir + "\n")
        print("There are empty directories!")
        print("Consult {} for more information".format(save_file))
    if not file_exts and not empty_dirs:
        print("Base directory is empty. No reports produced.")
        return 0


def main(args):
    file_exts, empty_dirs = explore_dir(args.path)
    report_results(file_exts, empty_dirs, args.path)


if __name__ == "__main__":
    try:
        parser = create_parser()
        args = parser.parse_args()
    except NotADirectoryError as e:
        print("{} is not a directory!".format(e))

    main(args)
