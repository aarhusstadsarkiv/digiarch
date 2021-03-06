# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------
from acamodels import ArchiveFile

from digiarch.core import fix_extensions
from digiarch.core.identify_files import sf_id

# -----------------------------------------------------------------------------
# Tests
# -----------------------------------------------------------------------------


class TestFixFiles:
    def test_mock_pdf(self, temp_dir):
        pdf_file = temp_dir / "mock.fail"
        pdf_file.write_bytes(bytes.fromhex("255044462D312E332525454F46"))
        pdf_id = sf_id(pdf_file)
        pdf_info = ArchiveFile(path=pdf_file, **pdf_id[pdf_file].dict())
        fix_extensions([pdf_info])
        files = list(temp_dir.rglob("*"))
        assert temp_dir / "mock.fail.pdf" in files
