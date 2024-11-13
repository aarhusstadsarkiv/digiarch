from pathlib import Path
from typing import ClassVar
from typing import Generator

from acacore.utils.functions import find_files
from acacore.utils.functions import rm_tree

# noinspection PyProtectedMember
from patoolib import extract_archive
from patoolib.util import PatoolError

from digiarch.commands.doctor import sanitize_path

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

    def extract(self) -> Generator[tuple[Path, Path], None, None]:
        extract_folder: Path = self.extract_folder
        extract_folder_tmp: Path = extract_folder.with_name(extract_folder.name + "_tmp")
        rm_tree(extract_folder_tmp)

        try:
            extract_archive(str(self.file.get_absolute_path()), outdir=str(extract_folder_tmp))

            for path in find_files(extract_folder_tmp):
                path_sanitized: Path = extract_folder / sanitize_path(path.relative_to(extract_folder_tmp))
                while path_sanitized.exists():
                    path_sanitized = path_sanitized.with_name("_" + path_sanitized.name)
                path_sanitized.parent.mkdir(parents=True, exist_ok=True)
                yield path.replace(path_sanitized), extract_folder.joinpath(path.relative_to(extract_folder_tmp))
        except PatoolError as err:
            if any("encrypted" in str(arg) for arg in err.args):
                raise PasswordProtectedError(self.file)
            raise ExtractError(err.args[0] if err.args else repr(err))
        finally:
            rm_tree(extract_folder_tmp)
