"""File database backend"""

# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------

import json
from typing import List
import sqlalchemy as sql
from databases import Database
from acamodels import ArchiveFile

# -----------------------------------------------------------------------------
# Database class
# -----------------------------------------------------------------------------


class FileDB(Database):
    """File database"""

    sql_meta = sql.MetaData()

    metadata = sql.Table(
        "Metadata",
        sql_meta,
        sql.Column("last_run", sql.DateTime, nullable=False),
        sql.Column("processed_dir", sql.String, nullable=False),
        sql.Column("file_count", sql.Integer),
        sql.Column("total_size", sql.Integer),
    )

    files = sql.Table(
        "Files",
        sql_meta,
        sql.Column("id", sql.Integer, primary_key=True, autoincrement=True),
        sql.Column("path", sql.String, nullable=False),
        sql.Column("checksum", sql.String),
        sql.Column("identification", sql.JSON),
    )

    def __init__(self, url: str) -> None:
        super().__init__(url)
        engine = sql.create_engine(
            url, connect_args={"check_same_thread": False}
        )
        self.sql_meta.create_all(engine)

    async def insert_files(self, files: List[ArchiveFile]) -> None:
        query = self.files.insert()
        await self.execute_many(
            query=query,
            values=[
                json.loads(file.json(exclude_none=True)) for file in files
            ],
        )
