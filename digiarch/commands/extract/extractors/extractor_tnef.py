from pathlib import Path
from typing import ClassVar
from typing import Generator

from tnefparse import TNEF

from digiarch.commands.doctor import sanitize_filename
from digiarch.commands.extract.extractors.base import ExtractorBase


class TNEFExtractor(ExtractorBase):
    tool_names: ClassVar[list[str]] = [
        "tnefparse",  # kept for backwards compatibility
        "tnef",
    ]

    def extract(self) -> Generator[tuple[Path, Path], None, None]:
        extract_folder: Path = self.extract_folder
        attachments_folder: Path = extract_folder.joinpath("attachments")
        extract_folder.mkdir(parents=True, exist_ok=True)

        with self.file.get_absolute_path().open("rb") as fh:
            tnef = TNEF(fh.read())

            for attachment in tnef.attachments:
                name: str = attachment.long_filename() or attachment.name
                path: Path = attachments_folder.joinpath(sanitize_filename(name))
                path.parent.mkdir(parents=True, exist_ok=True)
                with path.open("wb") as oh:
                    oh.write(attachment.data)
                    yield path, attachments_folder.joinpath(name)

            if tnef.htmlbody:
                body_path: Path = extract_folder.joinpath(f"{self.file.relative_path.stem}_body.html")
                body_path.write_text(tnef.htmlbody)
                yield body_path, body_path
            elif tnef.rtfbody:
                body_path: Path = extract_folder.joinpath(f"{self.file.relative_path.stem}_body.rtf")
                body_path.write_bytes(tnef.rtfbody)
                yield body_path, body_path
            elif tnef.body and isinstance(tnef.body, bytes):
                body_path: Path = extract_folder.joinpath(f"{self.file.relative_path.stem}_body.txt")
                body_path.write_bytes(tnef.body)
                yield body_path, body_path
            elif tnef.body:
                body_path: Path = extract_folder.joinpath(f"{self.file.relative_path.stem}_body.txt")
                body_path.write_text(tnef.body)
                yield body_path, body_path