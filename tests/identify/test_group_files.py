from pathlib import Path
from digiarch.utils.group_files import grouping
from digiarch.data import FileInfo, dump_file


class TestGrouping:
    def test_with_file(self, main_dir, data_file):
        file_1 = FileInfo(name="file1.txt", ext="txt", path="/root/file1.txt")
        file_2 = FileInfo(name="file2.pdf", ext="pdf", path="/root/file2.pdf")
        file_info = [file_1, file_2]
        dump_file(file_info, data_file)

        grouping(data_file, main_dir)
        contents = [str(p) for p in Path(main_dir).iterdir()]
        assert any("txt_files.txt" in content for content in contents)
        assert any("pdf_files.txt" in content for content in contents)
