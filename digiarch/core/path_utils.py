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


from digiarch.core.ArchiveFileRel import ArchiveFile
from digiarch.core.utils import natsort_path
from digiarch.core.utils import size_fmt
from digiarch.exceptions import FileCollectionError
from digiarch.models import FileData
from digiarch.models import Metadata
from tqdm import tqdm


# -----------------------------------------------------------------------------
# Function Definitions
# -----------------------------------------------------------------------------


async def explore_dir(file_data: FileData) -> List[str]:
    """Finds files and empty directories in the given path,
    and writes them to a file database.

    Parameters
    ----------
    file_data: models.FileData
        File data model including main directory, data directory,
        and file database.

    Returns
    ----------
    List[str]
        List of warnings encountered while parsing directories.

    """
    # Type declarations
    dir_info: List[ArchiveFile] = []
    empty_subs: List[Path] = []
    multiple_files: List[Path] = []
    warnings: List[str] = []
    total_size: int = 0
    file_count: int = 0
    main_dir: Path = file_data.main_dir
    data_dir: Path = file_data.data_dir
    metadata = Metadata(
        last_run=datetime.now(), processed_dir=file_data.main_dir
    )

    if not [
        child for child in main_dir.iterdir() if child.name != data_dir.name
    ]:
        # Path is empty, remove main directory and raise
        shutil.rmtree(data_dir)
        raise FileCollectionError(f"{main_dir} is empty! No files collected.")

    # Traverse given path, collect results.
    # tqdm is used to show progress of os.walk

    for root, dirs, files in tqdm(
        os.walk(main_dir, topdown=True), unit=" folders", desc="Processed"
    ):

        if data_dir.name in dirs:
            # Don't walk the _digiarch (_metadata) directory
            dirs.remove(data_dir.name)
        if not dirs and not files:
            # We found an empty subdirectory.
            empty_subs.append(Path(root))
        if len(files) > 1:
            multiple_files.append(Path(root))
        for file in files:
            try:
                cur_path = Path(root, file)
                print("Current path: {}".format(str(cur_path)))
                cur_rel_path = cur_path.relative_to(os.environ["ROOTPATH"])
                print("Root path from explore dir".format(os.environ["ROOTPATH"]))
                print("Path saved to db: ".format(str(cur_rel_path)))
                dir_info.append(ArchiveFile(relative_path=cur_rel_path))
            except Exception as e:
                raise FileCollectionError(e)
            else:
                total_size += cur_path.stat().st_size
                file_count += 1

    dir_info = natsort_path(dir_info)

    # Update metadata
    metadata.file_count = file_count
    metadata.total_size = size_fmt(total_size)

    # Update aux tables
    if empty_subs:
        await file_data.db.set_empty_subs(empty_subs)
        warnings.append("Warning! Empty subdirectories detected!")
    if multiple_files:
        await file_data.db.set_multi_files(multiple_files)
        warnings.append("Warning! Some directories have multiple files!")

    # Update db
    await file_data.db.set_metadata(metadata)
    await file_data.db.set_files(dir_info)

    return warnings
