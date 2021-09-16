"""Identify files using
`siegfried <https://github.com/richardlehane/siegfried>`
"""
# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------
import json
import re
import subprocess
import os
from functools import partial
from pathlib import Path
from typing import Any
from typing import Dict
from typing import List

from digiarch.core.ArchiveFileRel import ArchiveFile
from acamodels import Identification
from digiarch.core.utils import natsort_path
from digiarch.exceptions import IdentificationError


# -----------------------------------------------------------------------------
# Function Definitions
# -----------------------------------------------------------------------------


def update_file_id(
    path: Path, file_id: Identification, signature: Dict[str, str]
) -> None:
    file_id.puid = signature["puid"]
    file_id.signature = signature["signature"]
    if path.suffix.lower() != signature["extension"]:
        file_id.warning = "Extension mismatch"
    else:
        file_id.warning = None


def custom_id(path: Path, file_id: Identification) -> Identification:
    sig_file = Path(__file__).parent / "custom_sigs.json"
    signatures: List[Dict] = json.load(sig_file.open(encoding="utf-8"))

    with path.open("rb") as file_bytes:
        # BOF
        bof = file_bytes.read(1024).hex()
        # Navigate to EOF
        try:
            file_bytes.seek(-1024, 2)
        except OSError:
            # File too small :)
            file_bytes.seek(-file_bytes.tell(), 2)
        eof = file_bytes.read(1024).hex()

    for sig in signatures:
        if "bof" in sig and "eof" in sig:
            bof_pattern = re.compile(sig["bof"])
            eof_pattern = re.compile(sig["eof"])
            if sig["operator"] == "OR":
                if bof_pattern.search(bof) or eof_pattern.search(eof):
                    update_file_id(path, file_id, sig)
                    break
            elif sig["operator"] == "AND":
                if bof_pattern.search(bof) and eof_pattern.search(eof):
                    update_file_id(path, file_id, sig)
                    break
        elif "bof" in sig:
            bof_pattern = re.compile(sig["bof"])
            if bof_pattern.search(bof):
                update_file_id(path, file_id, sig)
                break
        elif "eof" in sig:
            eof_pattern = re.compile(sig["eof"])
            if eof_pattern.search(eof):
                update_file_id(path, file_id, sig)
                break
    return file_id


def sf_id(path: Path) -> Dict[Path, Identification]:
    """Identify files using
    `siegfried <https://github.com/richardlehane/siegfried>`_ and update
    FileInfo with obtained PUID, signature name, and warning if applicable.

    Parameters
    ----------
    path : pathlib.Path
        Path in which to identify files.

    Returns
    -------
    Dict[Path, Identification]
        Dictionary containing file path and associated identification
        information obtained from siegfried's stdout.

    Raises
    ------
    IdentificationError
        If running siegfried or loading of the resulting JSON output fails,
        an IdentificationError is thrown.
    """

    id_dict: Dict[Path, Identification] = {}

    try:
        sf_proc = subprocess.run(
            ["sf", "-json", "-multi", "1024", str(path)],
            check=True,
            capture_output=True,
        )
    except Exception as error:
        raise IdentificationError(error)

    try:
        id_result = json.loads(sf_proc.stdout)
    except Exception as error:
        raise IdentificationError(error)

    for file_result in id_result.get("files", []):
        match: Dict[str, Any] = {}
        for id_match in file_result.get("matches"):
            if id_match.get("ns") == "pronom":
                match = id_match
        if match:
            file_identification: Identification
            file_path: Path = Path(file_result["filename"])

            if match.get("id", "").lower() == "unknown":
                puid = None
            else:
                puid = match.get("id")

            signature = match.get("format")
            warning = match.get("warning", "").capitalize()
            file_identification = Identification(
                puid=puid, signature=signature or None, warning=warning or None
            )
            if puid is None:
                file_identification = custom_id(file_path, file_identification)

            # Possible MS Office files identified as markup (XML, HTML etc.)
            if (
                puid in ["fmt/96", "fmt/101", "fmt/583", "x-fmt/263"]
                and "Extension mismatch" in warning
            ):
                file_identification = custom_id(file_path, file_identification)
            id_dict.update({file_path: file_identification})

    return id_dict


def is_binary(file: ArchiveFile) -> bool:
    """
    Description:
    ----------------
    Checks if an ArchiveFile is a txt file or binary.
    This is done by looking for the null byte
    (text files wont include null bytes, since they are null terminated,
    i.e. the text in the text file stops at the null byte).
    We also check if the hexadecimal pdf signature is in the file
    since some pdf files might not include the null byte.
    """
    pdf_signature = "25504446"
    word_markup_puid = "aca-fmt/2"
    bytes_of_file = file.read_bytes()

    if b"\x00" in bytes_of_file:
        return True
    elif pdf_signature in bytes_of_file.hex():
        return True
    elif file.puid == word_markup_puid:
        return True
    else:
        return False


def update_file_info(
    file_info: ArchiveFile, id_info: Dict[Path, Identification]
) -> ArchiveFile:
    no_id: Identification = Identification(
        puid=None,
        signature=None,
        warning="No identification information obtained.",
    )
    file_path = Path(os.environ["ROOTPATH"], file_info.relative_path)
    new_id: Identification = id_info.get(file_path) or no_id

    if file_path.stat().st_size == 0:
        new_id = Identification(
            puid="aca-error/1",
            signature="Empty file",
            warning="Error: File is empty",
        )
    file_info = file_info.copy(update=new_id.dict())
    file_info.is_binary = is_binary(file_info)
    return file_info


def identify(files: List[ArchiveFile], path: Path) -> List[ArchiveFile]:
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

    id_info: Dict[Path, Identification] = sf_id(path)
    # functools.partial: Return a new partial object
    # which when called will behave like func called with the
    # positional arguments args and keyword arguments keywords.

    # Create a partial function of update_file_info
    # so that we can use map on it.
    # map cannot be used on update_file_info itself since id_info
    # can be shorter than files.
    _update = partial(update_file_info, id_info=id_info)
    updated_files: List[ArchiveFile] = list(map(_update, files))

    return natsort_path(updated_files)
