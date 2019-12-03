"""Module level docstring.

"""

# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------
import hashlib
from collections import Counter
from pathlib import Path
from typing import List, Set, Dict
import tqdm
from digiarch.data import FileInfo, get_fileinfo_list, dump_file

# -----------------------------------------------------------------------------
# Function Definitions
# -----------------------------------------------------------------------------


def file_checksum(file: Path) -> str:
    """Calculate the BLAKE2 checksum of an input file. Returns a 32 bit
    hex hash.

    Parameters
    ----------
    file : Path
        The file for which to calculate the checksum. Expects a `pathlib.Path`
        object.

    Returns
    -------
    str
        The 32 bit BLAKE2 hash of the input file.

    """

    # block_size: int = 524288000
    checksum: str = ""
    hasher: object = hashlib.blake2s()

    if file.is_file():
        with file.open("rb") as f:
            bytes = f.read()
            hasher.update(bytes)
            checksum = hasher.hexdigest()

    return checksum


def generate_checksums(data_file: str) -> None:
    """Generates BLAKE2 checksums of files in data_file.

    Parameters
    ----------
    data_file : str
        File from which to read and update information.
    """

    # Assign variables
    files: List[FileInfo] = get_fileinfo_list(data_file)
    updated_files: List[FileInfo] = []

    for file in tqdm.tqdm(files, desc="Generating Checksums", unit="files"):
        checksum = file_checksum(Path(file.path))
        updated_files.append(file.replace(checksum=checksum))

    dump_file(updated_files, data_file)


def check_collisions(data_file: str, save_path: str) -> int:
    """Generates a file with checksum collisions, indicating that duplicates
    are present.

    Parameters
    ----------
    data_file : str
        File from which to read and update information.
    save_path : str
        Path to which the checksum collision information should be saved.

    Returns
    -------
    int
        The number of collisions found.
    """

    # Initialise variables
    files: List[FileInfo] = get_fileinfo_list(data_file)
    checksums: List[str] = [file.checksum for file in files]
    collisions: Set[str] = set()
    file_collisions: Dict[str] = dict()
    checksum_counts: Counter = Counter(checksums).items()

    for checksum, count in tqdm.tqdm(checksum_counts):
        if count > 1:
            # We have a collision, boys
            collisions.add(checksum)

    for collision in collisions:
        file_hits = [file.path for file in files if file.checksum == collision]
        file_collisions.update({collision: file_hits})

    dups_file = Path(save_path).joinpath("duplicate_files.json")
    dump_file(file_collisions, dups_file)

    return len(collisions)
