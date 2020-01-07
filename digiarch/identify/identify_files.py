"""Module level docstring.

"""

# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------
import subprocess
from subprocess import CalledProcessError
from typing import List
from digiarch.data import FileInfo, Identification
from digiarch.utils.exceptions import IdentificationError
import yaml
import tqdm

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
                signame=match.get("format"), warning=match.get("warning"),
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

    # Assign variables
    updated_files: List[FileInfo] = []

    for file in tqdm.tqdm(files, desc="Identifying files", unit="files"):
        updated_files.append(sf_id(file))

    return updated_files
