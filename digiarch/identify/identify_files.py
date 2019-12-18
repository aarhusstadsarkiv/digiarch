"""Module level docstring.

"""

# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------
import subprocess
from digiarch.data import FileInfo, Identification
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
        puid=None, mime=None, warning="No identification information obtained."
    )
    cmd = subprocess.run(f"sf {file.path}", shell=True, capture_output=True)

    for doc in yaml.safe_load_all(cmd.stdout.decode()):
        if "matches" in doc:
            for match in doc.get("matches"):
                if match.get("ns") == "pronom":
                    if match.get("id").lower() == "unknown":
                        new_id = new_id.replace(warning=match.get("warning"))
                    else:
                        new_id = new_id.replace(
                            puid=match.get("id"),
                            mime=match.get("mime"),
                            warning=match.get("warning"),
                        )

    return file.replace(identification=new_id)


file = FileInfo(name="test_sheet", ext="", path=r"C:\data\test_data\cat.png",)
print(sf_id(file).to_dict().get("identification"))
