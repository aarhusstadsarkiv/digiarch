"""Identify files using
`siegfried <https://github.com/richardlehane/siegfried>`_

"""


# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------

import json
import subprocess
from functools import partial
from pathlib import Path
from typing import Any, Dict, List

from digiarch.exceptions import IdentificationError
from digiarch.internals import FileInfo, Identification, natsort_path

# -----------------------------------------------------------------------------
# Function Definitions
# -----------------------------------------------------------------------------


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
            signame = match.get("format")
            warning = match.get("warning", "").capitalize() or None

            if match.get("id", "").lower() == "unknown":
                puid = None
            else:
                puid = match.get("id")
            file_path: Path = Path(file_result["filename"])
            file_identification: Identification = Identification(
                puid=puid, signame=signame, warning=warning
            )
            id_dict.update({file_path: file_identification})

    return id_dict


def update_file_info(
    file_info: FileInfo, id_info: Dict[Path, Identification]
) -> FileInfo:
    no_id: Identification = Identification(
        puid=None,
        signame=None,
        warning="No identification information obtained.",
    )
    file_info.identification = id_info.get(file_info.path) or no_id
    return file_info


def identify(files: List[FileInfo], path: Path) -> List[FileInfo]:
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
    updated_files: List[FileInfo] = list(map(_update, files))

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
#         signame=None,
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
#             signame=match.get("format"), warning=match.get("warning")
#         )
#         if match.get("id", "").lower() == "unknown":
#             new_id.puid = None
#         else:
#             new_id.puid = match.get("id")
#         if isinstance(new_id.warning, str):
#             new_id.warning = new_id.warning.capitalize() or None

#     updated_file: FileInfo = file.replace(identification=new_id)
#     return updated_file
