# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------

from typing import List

from natsort import natsorted
from acamodels import ArchiveFile

# -----------------------------------------------------------------------------
# Utility functions
# -----------------------------------------------------------------------------


def size_fmt(size: float) -> str:
    """Formats a file size in binary multiples to a human readable string.

    Parameters
    ----------
    size : float
        The file size in bytes.

    Returns
    -------
    str
        Human readable string representing size in binary multiples.
    """
    for unit in ["B", "KiB", "MiB", "GiB", "TiB"]:
        if size < 1024.0:
            break
        size /= 1024.0
    return f"{size:.1f} {unit}"


def natsort_path(file_list: List[ArchiveFile]) -> List[ArchiveFile]:
    """Naturally sort a list of FileInfo objects by their paths.

    Parameters
    ----------
    file_list : List[FileInfo]
        The list of FileInfo objects to be sorted.

    Returns
    -------
    List[ArchiveFile]
        The list of FileInfo objects naturally sorted by their path.
    """

    sorted_file_list: List[ArchiveFile] = natsorted(
        file_list, key=lambda archive_file: str(archive_file.path)
    )

    return sorted_file_list
