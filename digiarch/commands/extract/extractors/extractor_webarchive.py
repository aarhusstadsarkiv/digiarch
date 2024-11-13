from pathlib import Path
from plistlib import load as load_plist
from typing import ClassVar
from typing import Generator
from urllib.parse import urlparse

from acacore.utils.functions import find_files
from acacore.utils.functions import rm_tree

from digiarch.commands.extract.extractors.base import ExtractError
from digiarch.commands.extract.extractors.base import ExtractorBase


class WebarchiveExtractor(ExtractorBase):
    tool_names: ClassVar[list[str]] = ["webarchive"]

    def extract(self) -> Generator[tuple[Path, Path], None, None]:
        extract_folder: Path = self.extract_folder
        extract_folder_tmp: Path = extract_folder.with_name(extract_folder.name + "_tmp")
        rm_tree(extract_folder_tmp)
        extract_folder_tmp.mkdir(parents=True, exist_ok=True)

        try:
            with self.file.get_absolute_path().open("rb") as fh:
                archive: dict = load_plist(fh)

            url_scheme: str = urlparse(archive["WebMainResource"]["WebResourceURL"]).scheme
            url_domain: str = urlparse(archive["WebMainResource"]["WebResourceURL"]).hostname
            url_prefix: str = f"{url_scheme}://{url_domain}/"
            archive["WebSubresources"] = archive.get("WebSubresources", [])

            index_file: Path = extract_folder_tmp.joinpath("index.html")
            index_file.write_bytes(archive["WebMainResource"]["WebResourceData"])

            for subframe in archive.get("WebSubframeArchives", []):
                if not subframe["WebMainResource"]["WebResourceURL"].startswith(url_prefix):
                    continue
                subframe_index: Path = extract_folder_tmp.joinpath(
                    subframe["WebMainResource"]["WebResourceURL"].removeprefix(url_prefix)
                )

                subframe_index.parent.mkdir(parents=True, exist_ok=True)
                subframe_index.write_bytes(subframe["WebMainResource"]["WebResourceData"])
                archive["WebSubresources"].extend(subframe["WebSubresources"])

            for resource in archive["WebSubresources"]:
                if not resource["WebResourceURL"].startswith(url_prefix):
                    continue
                resource_file: Path = extract_folder_tmp.joinpath(resource["WebResourceURL"].removeprefix(url_prefix))
                resource_file.parent.mkdir(parents=True, exist_ok=True)
                resource_file.write_bytes(resource["WebResourceData"])

            for file in find_files(extract_folder_tmp):
                file_new: Path = extract_folder.joinpath(file.relative_to(extract_folder_tmp))
                file_new.parent.mkdir(parents=True, exist_ok=True)
                file.replace(file_new)
                yield file_new, file_new
        except KeyError as e:
            raise ExtractError(self.file, "Malformed plist, KeyError", *e.args)
        finally:
            rm_tree(extract_folder_tmp)
