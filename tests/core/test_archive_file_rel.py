import pytest
from digiarch.core.ArchiveFileRel import ArchiveFile

class TestArchiveFileFunctions:
    def test_read_text(self, non_binary_file: ArchiveFile):
        with open(non_binary_file.get_absolute_path(), "r") as read_file:
            file_content = read_file.read()
        
        assert file_content == non_binary_file.read_text()

