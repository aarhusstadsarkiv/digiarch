from pathlib import Path
from typing import ClassVar
from typing import Generator
from zipfile import BadZipFile
from zipfile import LargeZipFile
from zipfile import ZipFile

from .base import ExtractError
from .base import ExtractorBase
from .base import PasswordProtectedError


class ZipExtractor(ExtractorBase):
    tool_names: ClassVar[list[str]] = ["zip"]

    def extract(self) -> Generator[Path, None, None]:
        extract_folder: Path = self.extract_folder

        try:
            with ZipFile(self.file.get_absolute_path()) as zf:
                for member in zf.infolist():
                    if member.is_dir():
                        continue
                    elif member.flag_bits & 0b1:
                        raise PasswordProtectedError(self.file)
                    extract_folder.joinpath(member.filename).parent.mkdir(parents=True, exist_ok=True)
                    yield Path(zf.extract(member, extract_folder))
        except (BadZipFile, LargeZipFile) as e:
            raise ExtractError(self.file, repr(e))
