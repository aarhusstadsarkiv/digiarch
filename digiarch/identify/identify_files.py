"""Module level docstring.

"""

# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------
import subprocess
from subprocess import CalledProcessError
from pathlib import Path
from digiarch.data import FileInfo, Identification
from digiarch.utils.exceptions import IdentificationError
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
    new_id: Identification = Identification(
        puid=None,
        signame=None,
        warning="No identification information obtained.",
    )
    try:
        cmd = subprocess.run(
            f"sf {file.path}", shell=True, capture_output=True, check=True
        )
    except CalledProcessError as error:
        raise IdentificationError(error)
    else:
        try:
            docs = yaml.safe_load_all(cmd.stdout.decode())
        except yaml.YAMLError as error:
            raise IdentificationError(error)
        else:
            match_doc = [
                doc.get("matches") for doc in docs if "matches" in doc
            ]
            # match_doc is a list of list of matches. Flatten it and get only
            # matches from PRONOM.
            matches = [
                match
                for matches in match_doc
                for match in matches
                if match.get("ns") == "pronom"
            ]
            for match in matches:
                if match.get("id").lower() == "unknown":
                    new_id = new_id.replace(
                        warning=match.get("warning").capitalize()
                    )
                else:
                    new_id = new_id.replace(
                        puid=match.get("id"),
                        signame=match.get("format"),
                        warning=match.get("warning").capitalize(),
                    )
    updated_file: FileInfo = file.replace(identification=new_id)
    return updated_file


file = FileInfo(
    name="test_sheet",
    ext="",
    path=Path(r"/home/jnik/Documents/test_data/test_sheet"),
)
print(sf_id(file).to_dict().get("identification"))
