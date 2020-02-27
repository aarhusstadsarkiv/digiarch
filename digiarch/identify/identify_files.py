"""Identify files using
`siegfried <https://github.com/richardlehane/siegfried>`_

"""

# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------
import subprocess
from multiprocessing import Pool
from subprocess import CalledProcessError
from typing import List
from digiarch.internals import FileInfo, Identification
from digiarch.exceptions import IdentificationError
import yaml
from tqdm import tqdm
from natsort import natsorted

# -----------------------------------------------------------------------------
# Function Definitions
# -----------------------------------------------------------------------------


def sf_id(file: FileInfo) -> FileInfo:
    """Identify files using
    `siegfried <https://github.com/richardlehane/siegfried>`_ and update
    FileInfo with obtained PUID, signature name, and warning if applicable.

    Parameters
    ----------
    file : FileInfo
        The file to identify.

    Returns
    -------
    updated_file : FileInfo
        Input file with updated information in the Identification field.

    Raises
    ------
    IdentificationError
        If running siegfried or loading of the resulting YAML output fails, an
        IdentificationError is thrown.

    """
    new_id: Identification = Identification(
        puid=None,
        signame=None,
        warning="No identification information obtained.",
    )
    try:
        cmd = subprocess.run(
            f'sf "{file.path}"', shell=True, capture_output=True, check=True
        )
    except CalledProcessError as error:
        raise IdentificationError(error)

    try:
        docs = yaml.safe_load_all(cmd.stdout.decode())
    except yaml.YAMLError as error:
        raise IdentificationError(error)
    else:
        match_doc = [doc.get("matches") for doc in docs if "matches" in doc]
        # match_doc is a list of list of matches. Flatten it and get only
        # matches from PRONOM.
        matches = [
            match
            for matches in match_doc
            for match in matches
            if match.get("ns") == "pronom"
        ]
        for match in matches:
            new_id = new_id.replace(
                signame=match.get("format"), warning=match.get("warning")
            )
            if match.get("id", "").lower() == "unknown":
                new_id.puid = None
            else:
                new_id.puid = match.get("id")
            if isinstance(new_id.warning, str):
                new_id.warning = new_id.warning.capitalize()

    updated_file: FileInfo = file.replace(identification=new_id)
    return updated_file


def identify(files: List[FileInfo]) -> List[FileInfo]:
    """Identify all files in a list, and return the updated list.

    Parameters
    ----------
    files : List[FileInfo]
        Files to identify.

    Returns
    -------
    List[FileInfo]
        Input files with updated Identification information.

    """

    updated_files: List[FileInfo]

    # Multiprocess identification
    with Pool() as p:
        updated_files = list(
            tqdm(
                p.imap_unordered(sf_id, files),
                desc="Identifying files",
                unit="files",
                total=len(files),
            )
        )

    # Natsort list by file.path
    updated_files = natsorted(
        updated_files, key=lambda fileinfo: str(fileinfo.path)
    )

    return updated_files
