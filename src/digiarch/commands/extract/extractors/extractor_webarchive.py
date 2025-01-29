from pathlib import Path
from plistlib import load as load_plist
from typing import ClassVar
from urllib.parse import urlparse

from acacore.utils.functions import find_files
from digiarch.commands.extract.extractors.base import ExtractError
from digiarch.commands.extract.extractors.base import ExtractorBase
from digiarch.common import TempDir


class WebarchiveExtractor(ExtractorBase):
    tool_names: ClassVar[list[str]] = ["webarchive"]

    def extract(self) -> list[tuple[Path, Path]]:
        extract_folder: Path = self.extract_folder
        files: list[tuple[Path, Path]] = []

        try:
            with self.file.get_absolute_path().open("rb") as fh:
                archive: dict = load_plist(fh)

            url_scheme: str = urlparse(archive["WebMainResource"]["WebResourceURL"]).scheme
            url_domain: str = urlparse(archive["WebMainResource"]["WebResourceURL"]).hostname
            url_prefix: str = f"{url_scheme}://{url_domain}/"
            archive["WebSubresources"] = archive.get("WebSubresources", [])

            with TempDir(self.file.root) as tmp_dir:
                index_file: Path = tmp_dir.joinpath("index.html")
                index_file.write_bytes(archive["WebMainResource"]["WebResourceData"])

                for subframe in archive.get("WebSubframeArchives", []):
                    if not subframe["WebMainResource"]["WebResourceURL"].startswith(url_prefix):
                        continue
                    subframe_index: Path = tmp_dir.joinpath(
                        subframe["WebMainResource"]["WebResourceURL"].removeprefix(url_prefix)
                    )

                    subframe_index.parent.mkdir(parents=True, exist_ok=True)
                    subframe_index.write_bytes(subframe["WebMainResource"]["WebResourceData"])
                    archive["WebSubresources"].extend(subframe["WebSubresources"])

                for resource in archive["WebSubresources"]:
                    if not resource["WebResourceURL"].startswith(url_prefix):
                        continue
                    resource_file: Path = tmp_dir.joinpath(resource["WebResourceURL"].removeprefix(url_prefix))
                    resource_file.parent.mkdir(parents=True, exist_ok=True)
                    resource_file.write_bytes(resource["WebResourceData"])

                for file in find_files(tmp_dir):
                    file_new: Path = extract_folder.joinpath(file.relative_to(tmp_dir))
                    file_new.parent.mkdir(parents=True, exist_ok=True)
                    files.append((file.replace(file_new), file_new))

            return files
        except KeyError as e:
            raise ExtractError(self.file, "Malformed plist, KeyError", *e.args)
