"""Utilities for handling files, paths, etc.

"""
# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------
import os
from typing import Tuple, List

# -----------------------------------------------------------------------------
# Function Definitions
# -----------------------------------------------------------------------------


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
