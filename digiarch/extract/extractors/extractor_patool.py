from pathlib import Path
from typing import ClassVar
from typing import Generator

from acacore.utils.functions import find_files

# noinspection PyProtectedMember
from patoolib import extract_archive
from patoolib.util import PatoolError

from .base import ExtractError
from .base import ExtractorBase
from .base import PasswordProtectedError


class PatoolExtractor(ExtractorBase):
    tool_names: ClassVar[list[str]] = [
        "patool",
        "7z",
        "ace",
        "adf",
        "alzip",
        "ape",
        "ar",
        "arc",
        "arj",
        "bzip2",
        "cab",
        "chm",
        "compress",
        "cpio",
        "deb",
        "dms",
        "flac",
        "gzip",
        "iso",
        "lrzip",
        "lzh",
        "lzip",
        "lzma",
        "lzop",
        "rar",
        "rpm",
        "rzip",
        "shar",
        "shn",
        "tar",
        "vhd",
        "xz",
        "zoo",
        "zpaq",
    ]

    def extract(self) -> Generator[Path, None, None]:
        extract_folder: Path = self.extract_folder

        try:
            extract_archive(str(self.file.get_absolute_path()), outdir=str(extract_folder))
        except PatoolError as err:
            if any("encrypted" in str(arg) for arg in err.args):
                raise PasswordProtectedError(self.file)
            raise ExtractError(err.args[0] if err.args else repr(err))

        yield from find_files(extract_folder)
