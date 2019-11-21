"""Module level docstring.

"""

# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------
import os
import json
from digiarch.data import get_fileinfo_list

# -----------------------------------------------------------------------------
# Function Definitions
# -----------------------------------------------------------------------------


def get_map(map_file: str) -> dict:
    with open(map_file, "r") as file:
        map_dict = json.load(file)
    return map_dict


def grouping(data_file: str, file_map: dict, save_path: str) -> None:
    """Function level documentation.
    Delete non-applicable sections.

    Parameters
    ----------
    input : type
        description

    Returns
    -------
    return : type
        description
    type (anonymous types are allowed in return)
        description
    Raises
    ------
    BadException
        description

    """

    convert_map: dict = {}
    for file in get_fileinfo_list(data_file):
        convert_tool = file_map.get(file.ext)
        convert_map.setdefault(convert_tool, []).append(f"{file.path}\n")

    for tool in convert_map:
        convert_tool_file = os.path.join(save_path, f"{tool}_files.txt")
        with open(convert_tool_file, "w") as file:
            file.writelines(convert_map.get(tool))
