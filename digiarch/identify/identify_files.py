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
    docs = yaml.safe_load_all(cmd.stdout)
    for doc in docs:
        if doc.get("matches"):
            return doc.get("matches")[0]


file = FileInfo(
    name="test_sheet",
    ext="",
    path="/home/jnik/Documents/test_data/test_sheet",
    checksum="",
)
sf_id(file)
