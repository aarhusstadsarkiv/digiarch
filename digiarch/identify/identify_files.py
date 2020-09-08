"""Identify files using
`siegfried <https://github.com/richardlehane/siegfried>`_

"""


# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------

import json
import re
import subprocess
from functools import partial
from pathlib import Path
from typing import Any, Dict, List

from acamodels import Identification
from digiarch.exceptions import IdentificationError
from digiarch.internals import ArchiveFile, natsort_path

# -----------------------------------------------------------------------------
# Function Definitions
# -----------------------------------------------------------------------------


def custom_id(path: Path, file_id: Identification) -> Identification:
    sig_lwp = re.compile(
        r"(?i)^576F726450726F0DFB000000000000"
        "000005985C8172030040CCC1BFFFBDF970"
    )
    sig_gif_bof = re.compile(r"(?i)^474946383961")
    sig_gif_eof = re.compile(r"(?i)3B")
    sig_123 = re.compile(r"(?i)^00001A000(3|4|5)10040000000000")
    sig_mmap = re.compile(r"(?i)4D696E644d616E61676572")
    sig_word_markup = re.compile(
        r"(?i)(50|70)726F67(49|69)64[0-9A-F]{2,20}576f72642e446f63756d656e74"
    )
    sig_excel_markup = re.compile(
        r"(?i)(50|70)726F67(49|69)64[0-9A-F]{2,18}457863656C2E5368656574"
    )
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

        if sig_lwp.match(bof):
            file_id.puid = "x-fmt/340"
            file_id.signature = "Lotus WordPro Document"
            if path.suffix.lower() != ".lwp":
                file_id.warning = "Extension mismatch"
            else:
                file_id.warning = None
        elif sig_123.match(bof):
            file_id.puid = "aca-fmt/1"
            file_id.signature = "Lotus 1-2-3 Spreadsheet"
            if path.suffix.lower() != ".123":
                file_id.warning = "Extension mismatch"
            else:
                file_id.warning = None
        elif sig_word_markup.search(bof):
            file_id.puid = "aca-fmt/2"
            file_id.signature = "Microsoft Word Markup"
            if path.suffix.lower() != ".doc":
                file_id.warning = "Extension mismatch"
            else:
                file_id.warning = None
        elif sig_excel_markup.search(bof):
            file_id.puid = "aca-fmt/3"
            file_id.signature = "Microsoft Excel Markup"
            if path.suffix.lower() != ".xls":
                file_id.warning = "Extension mismatch"
            else:
                file_id.warning = None
        elif sig_mmap.search(bof) or sig_mmap.search(eof):
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
    file_info.identification = id_info.get(file_info.path) or no_id
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
    _update = partial(update_file_info, id_info=id_info)
    updated_files: List[ArchiveFile] = list(map(_update, files))

    return natsort_path(updated_files)


# def identify(files: List[FileInfo]) -> List[FileInfo]:
#     """Identify all files in a list, and return the updated list.

#     Parameters
#     ----------
#     files : List[FileInfo]
#         Files to identify.

#     Returns
#     -------
#     List[FileInfo]
#         Input files with updated Identification information.

#     """

#     updated_files: List[FileInfo]

#     # Start siegfried server
#     servers: List[str] = [
#         f"localhost:{port}"
# for port in range(1337, 1337 + mp.cpu_count() * 2)
#     ]
#     sf_procs: List[subprocess.Popen] = []
#     for server in servers:
#         proc = subprocess.Popen(
#             ["sf", "-coe", "-serve", server],
#             # stdout=subprocess.DEVNULL,
#             # stderr=subprocess.DEVNULL,
#         )
#         sf_procs.append(proc)

#     # Multiprocess identification
#     pool = mp.Pool()
#     _identify = partial(sf_id, servers=servers)
#     try:
#         updated_files = list(
#             tqdm(
#                 pool.imap_unordered(_identify, files),
#                 desc="Identifying files",
#                 unit="files",
#                 total=len(files),
#             )
#         )
#     except KeyboardInterrupt:
#         pool.terminate()
#         pool.join()
#     finally:
#         pool.close()
#         pool.join()

#     # Close sf servers
#     for proc in sf_procs:
#         proc.terminate()
#         _, _ = proc.communicate()

#     # Natsort list by file.path
#     updated_files = natsort_path(updated_files)
#     return updated_files


# def sf_id(file: FileInfo, servers: List[str]) -> FileInfo:
#     """Identify files using
#     `siegfried <https://github.com/richardlehane/siegfried>`_ and update
#     FileInfo with obtained PUID, signature name, and warning if applicable.

#     Parameters
#     ----------
#     file : FileInfo
#         The file to identify.

#     Returns
#     -------
#     updated_file : FileInfo
#         Input file with updated information in the Identification field.

#     Raises
#     ------
#     IdentificationError
#         If running siegfried or loading of the resulting YAML output fails,
#         an IdentificationError is thrown.

#     """

#     new_id: Identification = Identification(
#         puid=None,
#         signature=None,
#         warning="No identification information obtained.",
#     )
#     server: str = random.choice(servers)
#     # with subprocess.Popen(["sf", "-serve", server]) as proc:

#     base64_path: str = urlsafe_b64encode(bytes(file.path)).decode()
#     id_response = requests.get(
#         f"http://{server}/identify/{base64_path}?base64=true&format=json"
#     )

#     try:
#         id_response.raise_for_status()
#     except HTTPError as error:
#         raise IdentificationError(error)
#     else:
#         id_result = id_response.json()

#     for file_result in id_result.get("files", []):
#         match: Dict[str, Any] = {}
#         for id_match in file_result.get("matches"):
#             if id_match.get("ns") == "pronom":
#                 match = id_match

#         new_id = new_id.replace(
#             signature=match.get("format"), warning=match.get("warning")
#         )
#         if match.get("id", "").lower() == "unknown":
#             new_id.puid = None
#         else:
#             new_id.puid = match.get("id")
#         if isinstance(new_id.warning, str):
#             new_id.warning = new_id.warning.capitalize() or None

#     updated_file: FileInfo = file.replace(identification=new_id)
#     return updated_file
