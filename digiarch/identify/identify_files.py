"""Identify files using
`siegfried <https://github.com/richardlehane/siegfried>`_

"""

# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------
import subprocess
import json
from functools import partial
from pathlib import Path
from subprocess import CalledProcessError
from typing import List, Any, Dict
from digiarch.internals import FileInfo, Identification, natsort_path
from digiarch.exceptions import IdentificationError
from halo import Halo
from tqdm import tqdm

# -----------------------------------------------------------------------------
# Function Definitions
# -----------------------------------------------------------------------------


def sf_id(path: Path) -> Dict[Path, Identification]:
    """Identify files in path using
    `siegfried <https://github.com/richardlehane/siegfried>`_ and return a list
    of corresponding file objects.

    Parameters
    ----------
    path : pathlib.Path
        The path in which to run siegfried.

    Returns
    -------
    identified_files : List[FileInfo]
        List of FileInfo objects with identification information.

    Raises
    ------
    IdentificationError
        If running siegfried fails, an IdentificationError is thrown.

    """
    new_id: Identification = Identification(
        puid=None,
        signame=None,
        warning="No identification information obtained.",
    )
    identified_files: Dict[Path, Identification] = {}
    try:
        cmd = subprocess.run(
            ["sf", "-json", "-multi", "256", path],
            capture_output=True,
            check=True,
        )
    except CalledProcessError as error:
        raise IdentificationError(error)
    else:
        id_result = json.loads(cmd.stdout.decode())

    for file in id_result.get("files", []):
        match: Dict[str, Any] = {}
        for id_match in file.get("matches"):
            if id_match.get("ns") == "pronom":
                match = id_match

        new_id = new_id.replace(
            signame=match.get("format"), warning=match.get("warning")
        )
        if match.get("id", "").lower() == "unknown":
            new_id.puid = None
        else:
            new_id.puid = match.get("id")
        if isinstance(new_id.warning, str):
            new_id.warning = new_id.warning.capitalize() or None
        identified_files.update({Path(file.get("filename", "")): new_id})

    return identified_files


def update_file(
    file: FileInfo, updated_files: Dict[Path, Identification]
) -> FileInfo:
    no_id = Identification(
        puid=None,
        signame=None,
        warning="No identification information obtained.",
    )
    file.identification = updated_files.get(file.path) or no_id
    return file


def identify(path: Path, files: List[FileInfo]) -> List[FileInfo]:
    """Identify all files in a list, and return the updated list.

    Parameters
    ----------
    path: pathlib.Path
        Path in which to identify files.
    files : List[FileInfo]
        Files to identify.

    Returns
    -------
    List[FileInfo]
        Input files with updated Identification information.

    """
    identified_files = sf_id(path)
    _update = partial(update_file, updated_files=identified_files)
    updated_files = list(map(_update, files))

    # Multiprocess identification
    # pool = Pool()
    # try:
    #     updated_files = list(
    #         tqdm(
    #             pool.imap_unordered(_update, files),
    #             desc="Identifying files",
    #             unit="files",
    #             total=len(files),
    #         )
    #     )
    # except KeyboardInterrupt:
    #     pool.terminate()
    #     pool.join()
    # finally:
    #     pool.close()
    #     pool.join()

    # Natsort list by file.path
    updated_files = natsort_path(updated_files)

    return updated_files
