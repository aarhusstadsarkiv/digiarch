"""Identify files using
`siegfried <https://github.com/richardlehane/siegfried>`_

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

from core.ArchiveFileRel import ArchiveFile
from acamodels import Identification
from digiarch.core.utils import natsort_path
from digiarch.exceptions import IdentificationError

# -----------------------------------------------------------------------------
# Function Definitions
# -----------------------------------------------------------------------------


def custom_id(path: Path, file_id: Identification) -> Identification:
    sig_gif_bof = re.compile(r"(?i)^474946383961")
    sig_gif_eof = re.compile(r"(?i)3B")
    sig_nsf_bof = re.compile(r"(?i)^1a000004000029000000")
    sig_nsf_eof = re.compile(r"(?i)bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
    sig_mmap = re.compile(r"(?i)4D696E644d616E61676572")
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
    if sig_mmap.search(bof) or sig_mmap.search(eof):
        file_id.puid = "aca-fmt/4"
        file_id.signature = "MindManager Mind Map"
        if path.suffix.lower() != ".mmap":
            file_id.warning = "Extension mismatch"
        else:
            file_id.warning = None
    elif sig_gif_bof.match(bof) and sig_gif_eof.search(eof):
        file_id.puid = "fmt/4"
        file_id.signature = "Graphics Interchange Format"
        if path.suffix.lower() != ".gif":
            file_id.warning = "Extension mismatch"
        else:
            file_id.warning = None
    elif path.suffix.lower() == ".id":
        file_id.puid = "aca-fmt/7"
        file_id.signature = "ID File"
        file_id.warning = "Match on extension only"
    elif sig_nsf_bof.search(bof) and sig_nsf_eof.search(eof):
        file_id.puid = "aca-fmt/8"
        file_id.signature = "Lotus Notes Database"
        if path.suffix.lower() != ".nsf":
            file_id.warning = "Extension mismatch"
        else:
            file_id.warning = None
    else:
        for sig in signatures:
            pattern = re.compile(sig["pattern"])
            if pattern.search(bof):
                file_id.puid = sig["puid"]
                file_id.signature = sig["signature"]
                if path.suffix.lower() != sig["extension"]:
                    file_id.warning = "Extension mismatch"
                else:
                    file_id.warning = None
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
    # functools.partial: Return a new partial object which when called will behave
    # like func called with the positional arguments args and keyword arguments keywords.

    # Create a partial function of update_file_info so that we can use map on it.
    # map cannot be used on update_file_info itself since id_info can be shorter than files.
    _update = partial(update_file_info, id_info=id_info)
    updated_files: List[ArchiveFile] = list(map(_update, files))

    return natsort_path(updated_files)
