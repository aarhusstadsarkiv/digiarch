from pathlib import Path
from sqlite3 import Cursor
from typing import Optional

from acacore.database import FileDB as FileDBBase


class FileDB(FileDBBase):
    # noinspection SqlResolve
    def file_exists(self, path: Path, root: Optional[Path] = None) -> bool:
        path = path.relative_to(root) if root else path
        cursor: Cursor = self.execute(
            f'select "{self.files.keys[0].name}" from "{self.files.name}" where relative_path = ? limit 1',
            [str(path)],
        )
        return cursor.fetchone() is not None
