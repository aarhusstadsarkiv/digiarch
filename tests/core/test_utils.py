# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------

from acamodels import ArchiveFile

from digiarch.core.utils import natsort_path, size_fmt

# -----------------------------------------------------------------------------
# Tests
# -----------------------------------------------------------------------------


def test_size_fmt():
    assert size_fmt(2 ** 0) == "1.0 B"
    assert size_fmt(2 ** 10) == "1.0 KiB"
    assert size_fmt(2 ** 20) == "1.0 MiB"
    assert size_fmt(2 ** 30) == "1.0 GiB"
    assert size_fmt(2 ** 40) == "1.0 TiB"


def test_natsort_path(xls_info, docx_info):
    xls_file = ArchiveFile(path=xls_info)
    docx_file = ArchiveFile(path=docx_info)
    assert natsort_path([xls_file, docx_file]) == [docx_file, xls_file]
