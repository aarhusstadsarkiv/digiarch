"""Module level docstring.

"""

# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------
import subprocess
from subprocess import CalledProcessError
from digiarch.data import FileInfo
import yaml

# -----------------------------------------------------------------------------
# Function Definitions
# -----------------------------------------------------------------------------


def sf_id(file: FileInfo) -> FileInfo:
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
    cmd = subprocess.run(f"sf {file.path}", shell=True, capture_output=True)

    for doc in yaml.safe_load_all(cmd.stdout.decode()):
        if "matches" in doc:
            for match in doc.get("matches"):
                if match.get("ns") == "pronom":
                    return match


file = FileInfo(
    name="test_sheet",
    ext="",
    path="/home/jnik/Documents/test_data/test_sheet",
)
print(sf_id(file))
print(file)
