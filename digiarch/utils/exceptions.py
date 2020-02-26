"""Custom exceptions defined for use in digiarch modules.

"""

# -----------------------------------------------------------------------------
# Classes
# -----------------------------------------------------------------------------


class DigiarchError(Exception):
    """Base class for digiarch errors."""


class IdentificationError(DigiarchError):
    """Implements an error to raise if identification or related
    functionality fails."""


class FileCollectionError(DigiarchError):
    """Implements an error to raise if File discovery/collection or related
    functionality fails."""
