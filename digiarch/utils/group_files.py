"""Module level docstring.

"""

# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------
import os
from typing import List
from digiarch.data import get_fileinfo_list, FileInfo

# -----------------------------------------------------------------------------
# Function Definitions
# -----------------------------------------------------------------------------


# def get_map(map_file: str) -> dict:
#     with open(map_file, "r") as file:
#         map_dict = json.load(file)
#     return map_dict


def grouping(data_file: str, save_path: str) -> None:
    """Groups files per file extension.

    Parameters
    ----------
    data_file : str
        File from which data is read.
    save_path : str
        Path to save results to
    """

    # Initialise variables
    files: List[FileInfo] = get_fileinfo_list(data_file)
    exts: List[str] = list(set([file.ext for file in files]))

    # Group files per file extension.
    for ext in exts:
        for file in files:
            if file.ext == ext:
                ext_file = os.path.join(save_path, f"{ext}_files.txt")
                with open(ext_file, "w") as out_file:
                    out_file.writelines(file.path)
