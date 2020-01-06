"""Utilities for handling files, paths, etc.

"""
# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------
import os
import shutil
from datetime import datetime
from pathlib import Path
from tqdm import tqdm
from digiarch.data import FileInfo, Metadata, FileData, dump_file
from digiarch.utils.exceptions import FileCollectionError
from typing import List

# -----------------------------------------------------------------------------
# Function Definitions
# -----------------------------------------------------------------------------


def size_fmt(size: float) -> str:
    for unit in ["B", "KiB", "MiB", "GiB", "TiB"]:
        if size < 1024.0:
            break
        size /= 1024.0
    return f"{size:.1f} {unit}"


def create_folders(*folder_paths: Path) -> None:
    """Creates given folders, and passes on FileExistsException.

    Parameters
    ----------
    *folder_paths : Path
        Paths of folders to create.

    """
    for folder in folder_paths:
        folder.mkdir(parents=True, exist_ok=True)


def explore_dir(path: Path, main_dir: Path, save_file: Path) -> bool:
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
    dir_info: List[FileInfo] = []
    empty_subs: List[Path] = []
    info: FileInfo
    ext: str
    total_size: int = 0
    file_count: int = 0
    main_dir_name: str = main_dir.resolve().name
    path_contents: List[Path] = [
        child for child in path.iterdir() if child.name != main_dir_name
    ]

    if not path_contents:
        # Path is empty, remove main directory and raise
        shutil.rmtree(main_dir)
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
        for file in files:
            cur_file = Path(file)
            ext = cur_file.suffix.lower()
            cur_path = Path(root, cur_file)
            size = cur_path.stat().st_size
            dir_info.append(
                FileInfo(
                    name=cur_file.name,
                    ext=ext,
                    path=cur_path,
                    size=size_fmt(size),
                )
            )
            total_size += size
            file_count += 1

    # Update metadata
    metadata = Metadata(
        created_on=datetime.now(),
        file_count=file_count,
        total_size=size_fmt(total_size),
        processed_dir=path,
    )

    if empty_subs:
        metadata.empty_subdirectories = empty_subs

    # Save results
    dump_file(data=FileData(metadata, dir_info), file=save_file)

    return bool(empty_subs)
