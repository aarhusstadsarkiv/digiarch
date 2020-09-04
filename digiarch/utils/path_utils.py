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

from tqdm import tqdm

from digiarch.exceptions import FileCollectionError
from digiarch.database import FileDB
from digiarch.internals import (
    ArchiveFile,
    FileData,
    Metadata,
    natsort_path,
    size_fmt,
)

# -----------------------------------------------------------------------------
# Function Definitions
# -----------------------------------------------------------------------------


async def explore_dir(path: Path, db: FileDB) -> None:
    """Finds files and empty directories in the given path,
    and writes them to a file database.

    Parameters
    ----------
    path : pathlib.Path
        The path in which to find files.

    db: FileDB
        File database to write results to

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
    # file_data = FileData(metadata=metadata)
    main_dir: Path = path / "_digiarch"
    if not [child for child in path.iterdir() if child.name != main_dir.name]:
        # Path is empty, remove main directory and raise
        shutil.rmtree(main_dir)
        raise FileCollectionError(f"{path} is empty! No files collected.")

    # Traverse given path, collect results.
    # tqdm is used to show progress of os.walk
    for root, dirs, files in tqdm(
        os.walk(path, topdown=True), unit=" folders", desc="Processed"
    ):
        if main_dir.name in dirs:
            # Don't walk the _digiarch directory
            dirs.remove(main_dir.name)
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

    dir_info = natsort_path(dir_info)
    # Update metadata
    metadata.file_count = file_count
    metadata.total_size = size_fmt(total_size)

    # TODO
    # empty dirs/multiple files from database
    # check with first()
    # if empty_subs:
    #     metadata.empty_subdirs = empty_subs
    # if several_files:
    #     metadata.several_files = several_files

    # Update file data
    await db.set_metadata(metadata)
    await db.set_files(dir_info)
