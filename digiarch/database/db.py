"""File database backend"""

# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------

import re
from typing import List

import sqlalchemy as sql
from databases import Database
from pydantic import parse_obj_as
from sqlalchemy.exc import OperationalError

from acamodels import ArchiveFile
from sqlalchemy_utils import create_view

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
        sql.Column("uuid", sql.String, nullable=False),
        sql.Column("path", sql.String, nullable=False),
        sql.Column("checksum", sql.String),
        sql.Column("puid", sql.String),
        sql.Column("signature", sql.String),
        sql.Column("warning", sql.String),
    )

    id_warnings = files.select().where(files.c.warning.isnot(None))
    puid_none = sql.case(
        [(files.c.puid.is_(None), "None")],
        else_=files.c.puid,
    )
    sig_count = (
        sql.select(
            [
                files.c.puid,
                files.c.signature,
                sql.func.count(puid_none).label("count"),
            ]
        )
        .group_by("puid")
        .order_by(sql.desc("count"))
    )
    create_view("IdentificationWarnings", id_warnings, sql_meta)
    create_view("SignatureCount", sig_count, sql_meta)

    def __init__(self, url: str) -> None:
        super().__init__(url)
        engine = sql.create_engine(
            url, connect_args={"check_same_thread": False}
        )
        try:
            self.sql_meta.create_all(engine)
        except OperationalError as error:
            warn_re = re.compile(
                r"(?i)(IdentificationWarnings|SignatureCount)"
            )
            if warn_re.search(str(error)):
                pass
            else:
                print(str(error))
                raise

    async def insert_files(self, files: List[ArchiveFile]) -> None:
        query = self.files.insert()
        encoded_files = [file.encode() for file in files]
        async with self.transaction():
            await self.execute_many(query=query, values=encoded_files)

    async def get_files(self) -> List[ArchiveFile]:
        query = self.files.select()
        rows = await self.fetch_all(query)
        files = parse_obj_as(List[ArchiveFile], rows)
        return files

    async def update_files(self, files: List[ArchiveFile]) -> None:
        async with self.transaction():
            for file in files:
                query = (
                    self.files.update()
                    .where(self.files.c.uuid == str(file.uuid))
                    .values(file.encode())
                )
                await self.execute(query)
