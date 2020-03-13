"""Identify files using
`siegfried <https://github.com/richardlehane/siegfried>`_

"""

# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------
import subprocess
import multiprocessing as mp
import requests
import asyncio
import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from requests import HTTPError
from base64 import urlsafe_b64encode
from functools import partial
from typing import Dict, Any, List, Union
from subprocess import CalledProcessError
from digiarch.internals import FileInfo, Identification, natsort_path
from digiarch.exceptions import IdentificationError
from tqdm import tqdm

# -----------------------------------------------------------------------------
# Function Definitions
# -----------------------------------------------------------------------------


# async def run_cmd(cmd: str) -> Any:
#     proc = await asyncio.create_subprocess_shell(
#         cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
#     )
#     std_out, std_err = await proc.communicate()


def sf_id(file: FileInfo, server: str) -> FileInfo:
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
    # base64_path = urlsafe_b64encode(bytes(file.path)).decode()
    # id_response = requests.get(
    #     f"http://{server}/identify/{base64_path}?base64=true&format=json"
    # )
    try:
        proc = subprocess.Popen(
            ["sf", "-json", file.path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
    except Exception as error:
        raise IdentificationError(error)

    std_out, std_err = proc.communicate()
    if proc.returncode != 0:
        raise IdentificationError(std_err.decode())
    else:
        id_result = json.loads(std_out.decode())
    # try:
    #     id_response.raise_for_status()
    # except HTTPError as error:
    #     raise IdentificationError(error)
    # else:
    #     id_result = id_response.json()

    for file_result in id_result.get("files", []):
        match: Dict[str, Any] = {}
        for id_match in file_result.get("matches"):
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

    # Start siegfried server
    server = "localhost:1337"
    # subprocess.Popen(
    #     ["sf", "-serve", server],
    #     stdout=subprocess.DEVNULL,
    #     stderr=subprocess.DEVNULL,
    # )

    # Multiprocess identification
    # mp.set_start_method("spawn")
    pool = mp.Pool()
    # paths = [file.path for file in files]
    _identify = partial(sf_id, server=server)
    with ThreadPoolExecutor() as executor:
        updated_files = list(
            tqdm(
                executor.map(_identify, files),
                desc="Identifying files",
                unit="files",
                total=len(files),
            )
        )
    # try:
    #     updated_files = list(
    #         tqdm(
    #             pool.imap_unordered(_identify, files),
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
