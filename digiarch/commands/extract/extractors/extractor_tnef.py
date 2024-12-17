from pathlib import Path
from typing import ClassVar

from tnefparse import TNEF

from digiarch.common import sanitize_filename
from digiarch.common import TempDir

from .base import ExtractorBase


class TNEFExtractor(ExtractorBase):
    tool_names: ClassVar[list[str]] = [
        "tnefparse",  # kept for backwards compatibility
        "tnef",
    ]

    def extract(self) -> list[tuple[Path, Path]]:
        extract_folder: Path = self.extract_folder
        files: list[tuple[str, str]] = []

        with self.file.get_absolute_path().open("rb") as fh:
            tnef = TNEF(fh.read())

        with TempDir(self.file.root) as tmp_dir:
            for attachment in tnef.attachments:
                name: str = attachment.long_filename() or attachment.name
                path: Path = tmp_dir.joinpath(sanitize_filename(name, 20, True))
                with path.open("wb") as oh:
                    oh.write(attachment.data)
                files.append((path.name, name))

            if not files:
                return []

            extract_folder.mkdir(parents=True, exist_ok=True)

            return [
                (
                    tmp_dir.joinpath(name_extracted).replace(extract_folder.joinpath(name_extracted)),
                    extract_folder.joinpath("_").with_name(name_original),
                )
                for name_extracted, name_original in files
            ]
