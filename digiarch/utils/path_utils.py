"""Utilities for handling files, paths, etc.

"""

# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------

import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import List

from digiarch.exceptions import FileCollectionError
from digiarch.internals import (
    ArchiveFile,
    FileData,
    Metadata,
    natsort_path,
    size_fmt,
)
from tqdm import tqdm

# -----------------------------------------------------------------------------
# Function Definitions
# -----------------------------------------------------------------------------


def explore_dir(path: Path) -> FileData:
    """Finds files and empty directories in the given path,
    and collects them into a list of FileInfo objects.

    Parameters
    ----------
    path : str
        The path in which to find files.

    Returns
    -------
    empty_subs: List[str]
        A list of empty subdirectory paths, if any such were found
    """
    # Type declarations
    dir_info: List[ArchiveFile] = []
    empty_subs: List[Path] = []
    several_files: List[Path] = []
    total_size: int = 0
    file_count: int = 0
    metadata = Metadata(last_run=datetime.now(), processed_dir=path)
    file_data = FileData(metadata=metadata)
    main_dir_name: str = file_data.digiarch_dir.name

    if not [child for child in path.iterdir() if child.name != main_dir_name]:
        # Path is empty, remove main directory and raise
        shutil.rmtree(file_data.digiarch_dir)
        raise FileCollectionError(f"{path} is empty! No files collected.")

    # Traverse given path, collect results.
    # tqdm is used to show progress of os.walk
    for root, dirs, files in tqdm(
        os.walk(path, topdown=True), unit=" folders", desc="Processed"
    ):
        if main_dir_name in dirs:
            # Don't walk the _digiarch directory
            dirs.remove(main_dir_name)
        if not dirs and not files:
            # We found an empty subdirectory.
            empty_subs.append(Path(root))
        if len(files) > 1:
            several_files.append(Path(root))
        for file in files:
            cur_path = Path(root, file)
            dir_info.append(ArchiveFile(path=cur_path))
            total_size += cur_path.stat().st_size
            file_count += 1

    # Update metadata
    metadata.file_count = file_count
    metadata.total_size = size_fmt(total_size)

    if empty_subs:
        metadata.empty_subdirs = empty_subs
    if several_files:
        metadata.several_files = several_files

    # Update file data
    file_data.metadata = metadata
    file_data.files = natsort_path(dir_info)

    # Save file data
    file_data.dump()

    return file_data
