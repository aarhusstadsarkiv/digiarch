"""Utilities for handling files, paths, etc.

"""
# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------
import os
from tqdm import tqdm
from digital_archive.data import FileInfo
from typing import List


# -----------------------------------------------------------------------------
# Function Definitions
# -----------------------------------------------------------------------------


def explore_dir(path: str) -> List[FileInfo]:
    """Finds files and empty directories in the given path,
    and collects them into a list of FileInfo objects.

    Parameters
    ----------
    path : str
        The path in which to find files.

    Returns
    -------
    List[FileInfo]
        List of :class:`~digital_archive.data.FileInfo` objects.

    """
    # Type declarations
    dir_info: List[FileInfo] = []
    info: FileInfo
    ext: str

    # Traverse given path, collect results.
    # tqdm is used to show progress of os.walk
    for root, dirs, files in tqdm(
        os.walk(path), unit=" folders", desc="Processed"
    ):
        if not dirs and not files:
            # We found an empty subdirectory.
            info = FileInfo(is_empty_sub=True, path=root)
            dir_info.append(info)
        for file in files:
            ext = os.path.splitext(file)[1].lower()
            info = FileInfo(name=file, ext=ext, path=root)
            dir_info.append(info)

    return dir_info
