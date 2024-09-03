from pathlib import Path
from typing import ClassVar
from typing import Generator
from zipfile import BadZipFile
from zipfile import LargeZipFile
from zipfile import ZipFile

from acacore.utils.functions import rm_tree

from digiarch.doctor import sanitize_path

from .base import ExtractError
from .base import ExtractorBase
from .base import PasswordProtectedError


class ZipExtractor(ExtractorBase):
    tool_names: ClassVar[list[str]] = [
        "zip",
        "kmz",
    ]

    def extract(self) -> Generator[Path, None, None]:
        extract_folder: Path = self.extract_folder
        extract_folder_tmp: Path = extract_folder.with_name(extract_folder.name + "_tmp")

        try:
            with ZipFile(self.file.get_absolute_path()) as zf:
                for member in zf.infolist():
                    if member.is_dir():
                        continue
                    elif member.flag_bits & 0b1:
                        raise PasswordProtectedError(self.file)
                    path: Path = Path(zf.extract(member, extract_folder_tmp))
                    path_sanitized: Path = extract_folder / sanitize_path(path.relative_to(extract_folder_tmp))
                    path_sanitized.parent.mkdir(parents=True, exist_ok=True)
                    yield path.replace(path_sanitized)
        except (BadZipFile, LargeZipFile) as e:
            raise ExtractError(self.file, repr(e))
        finally:
            rm_tree(extract_folder_tmp)
