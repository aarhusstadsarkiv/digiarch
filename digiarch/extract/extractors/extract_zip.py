from pathlib import Path
from typing import ClassVar
from typing import Generator
from zipfile import ZipFile

from .base import ExtractorBase
from .base import PasswordProtectedError


class ZipExtractor(ExtractorBase):
    tool_name: ClassVar[list[str]] = ["zip"]

    def extract(self) -> Generator[Path, None, None]:
        extract_folder: Path = self.extract_folder

        with ZipFile(self.file.get_absolute_path()) as zf:
            for member in zf.infolist():
                if member.is_dir():
                    continue
                elif member.flag_bits & 0b1:
                    raise PasswordProtectedError(self.file)
                extract_folder.joinpath(member.filename).parent.mkdir(parents=True, exist_ok=True)
                yield Path(zf.extract(member, extract_folder))
