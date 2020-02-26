"""This module implements checksum generation and duplicate detection.

"""

# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------
import hashlib
from collections import Counter
from pathlib import Path
from typing import List, Set, Dict, ItemsView, Any
import tqdm
import xxhash
from digiarch.internals import FileInfo, to_json

# -----------------------------------------------------------------------------
# Function Definitions
# -----------------------------------------------------------------------------


def file_checksum(file: Path, secure: bool = False) -> str:
    """Calculate the checksum of an input file using xxHash or BLAKE2,
    depending on need for cryptographical security.

    Parameters
    ----------
    file : Path
        The file for which to calculate the checksum. Expects a `pathlib.Path`
        object.
    secure : bool
        Whether the hash function used to generate checksums should be
        cryptographically secure.
        If false (the default), xxHash is used. If true, BLAKE2 is used.

    Returns
    -------
    str
        The hex checksum of the input file.

    """

    checksum: str = ""
    hasher: Any

    if secure:
        hasher = hashlib.blake2b()
    else:
        hasher = xxhash.xxh64(seed=42)

    if file.is_file():
        with file.open("rb") as f:
            hasher.update(f.read())
            checksum = hasher.hexdigest()

    return checksum


def generate_checksums(
    files: List[FileInfo], secure: bool = False, disable_progress: bool = False
) -> List[FileInfo]:
    """Generates checksums of files in data_file.

    Parameters
    ----------
    files : List[FileInfo]
        List of files that need checksums.
    secure : bool
        Whether the checksum generated should come from a cryptographically
        secure hashing function. Defaults to false.
    """

    # Assign variables
    updated_files: List[FileInfo] = []

    for file in tqdm.tqdm(
        files,
        desc="Generating checksums",
        unit="files",
        disable=disable_progress,
    ):
        checksum = file_checksum(Path(file.path), secure)
        updated_files.append(file.replace(checksum=checksum))

    return updated_files


def check_collisions(checksums: List[str]) -> Set[str]:
    checksum_counts: ItemsView[str, int] = Counter(checksums).items()
    collisions: Set[str] = set()

    for checksum, count in checksum_counts:
        if count > 1:
            # We have a collision, boys
            collisions.add(checksum)

    return collisions


def check_duplicates(files: List[FileInfo], save_path: Path) -> None:
    """Generates a file with checksum collisions, indicating that duplicates
    are present.

    Parameters
    ----------
    files : List[FileInfo]
        Files for which duplicates should be checked.
    save_path : Path
        Path to which the checksum collision information should be saved.
    """

    # Initialise variables
    # files: List[FileInfo] = get_fileinfo_list(data_file)
    possible_dups: List[FileInfo] = []
    checksums: List[str] = [
        file.checksum for file in files if file.checksum is not None
    ]
    collisions: Set[str] = check_collisions(checksums)
    file_collisions: Dict[str, str] = dict()
    # checksum_counts: Counter = Counter(checksums).items()

    for checksum in tqdm.tqdm(collisions, desc="Processing collisions"):
        # Generate secure checksums for possible file collisions.
        new_files = generate_checksums(
            [file for file in files if file.checksum == checksum],
            secure=True,
            disable_progress=True,
        )
        for file in new_files:
            possible_dups.append(file)

    secure_collisions: Set[str] = check_collisions(
        [file.checksum for file in possible_dups if file.checksum is not None]
    )
    for checksum in tqdm.tqdm(secure_collisions, desc="Finding duplicates"):
        hits = [
            {"name": file.name, "path": file.path}
            for file in possible_dups
            if file.checksum == checksum
        ]
        file_collisions.update({checksum: hits})

    dups_file = Path(save_path).joinpath("duplicate_files.json")
    to_json(file_collisions, dups_file)
