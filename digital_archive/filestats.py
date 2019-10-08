"""Module for finding and fixing files in archive hand-ins.

Functions
---------
find_files
    Finds files and empty directories in the given path.

"""

import os

# import argparse


def find_files():
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
    path = "/home/jnik/Documents/data/archive_data/batch_1/AVID.AARS.9.1_OriginalFiler"
    for root, dirs, files in os.walk(path):
        for f in files:
            ext = os.path.splitext(f)[1]
            file_exts.append(ext.lower())
        if not dirs and not files:
            empty_dirs.append(root)
    return file_exts, empty_dirs


def main():
    find_files()
    return 1
