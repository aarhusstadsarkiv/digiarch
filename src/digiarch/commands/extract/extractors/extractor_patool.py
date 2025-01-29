from pathlib import Path
from typing import ClassVar

from acacore.utils.functions import find_files
from digiarch.common import sanitize_filename
from digiarch.common import sanitize_path
from digiarch.common import TempDir
from patoolib import extract_archive
from patoolib.util import PatoolError

from .base import ExtractError
from .base import ExtractorBase
from .base import PasswordProtectedError


class PatoolExtractor(ExtractorBase):
    # noinspection SpellCheckingInspection
    tool_names: ClassVar[list[str]] = [
        "patool",  # kept for backwards compatibility
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

    def extract(self) -> list[tuple[Path, Path]]:
        extract_folder: Path = self.extract_folder
        files: list[tuple[Path, Path]] = []

        try:
            with TempDir(self.file.root) as tmp_dir:
                extract_archive(str(self.file.get_absolute_path()), outdir=str(tmp_dir), verbosity=-1)

                for path in find_files(tmp_dir):
                    path_sanitized: Path = extract_folder / sanitize_path(path.relative_to(tmp_dir))
                    path_sanitized = path_sanitized.with_name(sanitize_filename(path_sanitized.name, 20, True))
                    while path_sanitized.exists():
                        path_sanitized = path_sanitized.with_name("_" + path_sanitized.name)
                    path_sanitized.parent.mkdir(parents=True, exist_ok=True)
                    files.append((path.replace(path_sanitized), extract_folder.joinpath(path.relative_to(tmp_dir))))

            return files
        except PatoolError as err:
            if any("encrypted" in str(arg) for arg in err.args):
                raise PasswordProtectedError(self.file)
            raise ExtractError(err.args[0] if err.args else repr(err))
