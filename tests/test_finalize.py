from math import ceil
from pathlib import Path

import pytest
from acacore.database import FilesDB
from acacore.models.file import ConvertedFile
from click import BadParameter

from digiarch.cli import app
from digiarch.common import AVID
from tests.conftest import run_click


def test_finalize_docs_collections(avid_folder_copy: Path):
    avid = AVID(avid_folder_copy)

    with FilesDB(avid.database_path) as database:
        files: list[ConvertedFile] = database.statutory_files.select().fetchall()

    docs_in_collections: int = 3
    expected_paths: dict[Path, Path] = {
        f.relative_path: Path(
            avid.dirs.documents.name,
            f"docCollection{ceil(n / docs_in_collections)}",
            str(n),
            f"1{f.suffix}",
        )
        for n, f in enumerate(sorted(files, key=lambda f: f.relative_path), 1)
    }

    run_click(avid_folder_copy, app, "finalize", "doc-collections", "--docs-in-collection", docs_in_collections)

    with FilesDB(avid.database_path) as database:
        for file in files:
            test_file: ConvertedFile | None = database.statutory_files[{"uuid": str(file.uuid)}]
            assert test_file
            assert test_file.relative_path == expected_paths[file.relative_path]


def test_finalize_doc_index(avid_folder_copy: Path):
    run_click(avid_folder_copy, app, "finalize", "doc-index", "--media-id", "AVID.SA.19001.1")

    with pytest.raises(BadParameter, match=f"{'invalid'!r} not in format"):
        run_click(avid_folder_copy, app, "finalize", "doc-index", "--media-id", "invalid")
