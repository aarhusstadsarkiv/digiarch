# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------


from functools import lru_cache
from typing import Any, Optional, Tuple

import httpx
from natsort import natsorted

from digiarch.core.ArchiveFileRel import ArchiveFile

# -----------------------------------------------------------------------------
# Utility functions
# -----------------------------------------------------------------------------


def size_fmt(size: float) -> str:
    """Formats a file size in binary multiples to a human readable string.

    Parameters
    ----------
    size : float
        The file size in bytes.

    Returns:
    -------
    str
        Human readable string representing size in binary multiples.
    """
    for unit in ["B", "KiB", "MiB", "GiB", "TiB"]:
        if size < 1024.0:
            break
        size /= 1024.0
    return f"{size:.1f} {unit}"


def natsort_path(file_list: list[ArchiveFile]) -> list[ArchiveFile]:
    """Naturally sort a list of FileInfo objects by their paths.

    Parameters
    ----------
    file_list : List[FileInfo]
        The list of FileInfo objects to be sorted.

    Returns:
    -------
    List[ArchiveFile]
        The list of FileInfo objects naturally sorted by their path.
    """
    sorted_file_list: list[ArchiveFile] = natsorted(
        file_list,
        key=lambda archive_file: str(archive_file.relative_path),
    )

    return sorted_file_list


@lru_cache
def to_re_identify() -> ():
    """Gets the json file with the different formats that we wish to reidentify.

    Is kept updated on the reference-files repo. The function caches the result,
    soo multiple calls in the same run should not be an issue.
    """
    response = httpx.get(
        "https://raw.githubusercontent.com/aarhusstadsarkiv/reference-files/add-version-to-files/to_convert.json",
    )
    if response.status_code != 200:
        raise ConnectionError

    response_dict: dict[str, Any] = response.json()

    if response_dict is None:
        raise ConnectionError

    data_map: Optional[dict[str, Any]] = response_dict.get("data")
    version: Optional[str] = response_dict.get("version")

    if data_map is None:
        raise ValueError(
            "No data key in the respons. Seems to be a problem with to_convert on ref. repo",
        )
    if version is None:
        version = "Not specified by reference repo"

    return (data_map, version)


@lru_cache
def costum_sigs() -> ():
    """Gets the json file with our own costum formats in a list.

    Is kept updated on the reference-files repo. The function caches the result,
    soo multiple calls in the same run should not be an issue.
    """
    response = httpx.get(
        "https://raw.githubusercontent.com/aarhusstadsarkiv/reference-files/add-version-to-files/custom_signatures.json",
    )
    if response.status_code != 200:
        raise ConnectionError

    response_dict: dict[str, Any] = response.json()

    if response_dict is None:
        raise ConnectionError

    data_map: Optional[list[dict]] = response_dict.get("data")
    version: Optional[str] = response_dict.get("version")

    if data_map is None:
        raise ValueError(
            "No data key in the respons. Seems to be a problem with to_convert on ref. repo",
        )
    if version is None:
        version = "Not specified by reference repo"

    return (data_map, version)
