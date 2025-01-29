from pathlib import Path
from typing import ClassVar
from zipfile import BadZipFile
from zipfile import LargeZipFile
from zipfile import ZipFile

from digiarch.common import sanitize_filename
from digiarch.common import sanitize_path
from digiarch.common import TempDir

from .base import ExtractError
from .base import ExtractorBase
from .base import PasswordProtectedError


class ZipExtractor(ExtractorBase):
    tool_names: ClassVar[list[str]] = [
        "zip",
        "kmz",
    ]

    def extract(self) -> list[tuple[Path, Path]]:
        extract_folder: Path = self.extract_folder
        files: list[tuple[Path, Path]] = []
        files_tmp: list[tuple[Path, Path]] = []

        try:
            with (
                TempDir(self.file.root) as tmp_extract,
                TempDir(self.file.root) as tmp_final,
            ):
                with ZipFile(self.file.get_absolute_path()) as zf:
                    for member in zf.infolist():
                        if member.is_dir():
                            continue
                        if member.flag_bits & 0b1:
                            raise PasswordProtectedError(self.file)

                        path_original: Path = Path(zf.extract(member, tmp_extract))
                        path_final: Path = tmp_final.joinpath(sanitize_path(path_original.relative_to(tmp_extract)))
                        path_final = path_final.with_name(sanitize_filename(path_final.name, 20, True))
                        while path_final.exists():
                            path_final = path_final.with_name("_" + path_final.name)

                        path_final.parent.mkdir(parents=True, exist_ok=True)
                        path_original.replace(path_final)

                        files_tmp.append((path_final.relative_to(tmp_final), path_original.relative_to(tmp_extract)))

                for path_extracted, path_original in files_tmp:
                    extract_folder.joinpath(path_extracted).parent.mkdir(parents=True, exist_ok=True)
                    tmp_final.joinpath(path_extracted).replace(extract_folder.joinpath(path_extracted))
                    files.append((extract_folder.joinpath(path_extracted), extract_folder.joinpath(path_original)))

                return files
        except (BadZipFile, LargeZipFile) as e:
            raise ExtractError(self.file, repr(e))
