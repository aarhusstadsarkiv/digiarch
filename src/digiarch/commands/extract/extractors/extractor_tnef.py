from pathlib import Path
from typing import ClassVar

from digiarch.common import TempDir
from tnefparse import TNEF

from .base import ExtractorBase
from .extractor_msg import prepare_attachment_name


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
            names: list[str] = []
            for n, attachment in enumerate(tnef.attachments):
                name: str = attachment.long_filename() or attachment.name
                names, name, name_sanitized = prepare_attachment_name(names, name, n)
                with tmp_dir.joinpath(name_sanitized).open("wb") as oh:
                    oh.write(attachment.data)
                files.append((name_sanitized, name))

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
