from logging import INFO
from math import ceil
from pathlib import Path
from re import match
from xml.sax.saxutils import escape

from acacore.models.event import Event
from acacore.utils.click import ctx_params
from acacore.utils.click import end_program
from acacore.utils.click import start_program
from acacore.utils.helpers import ExceptionManager
from click import BadParameter
from click import command
from click import Context
from click import IntRange
from click import option
from click import pass_context
from pydantic import BaseModel
from pydantic import UUID4

from digiarch.__version__ import __version__
from digiarch.common import get_avid
from digiarch.common import open_database


class DocIndexFile(BaseModel):
    doc_id: int
    uuid: UUID4
    relative_path: Path
    original_uuid: UUID4
    original_path: Path
    parent_uuid: UUID4 | None
    parent_doc_id: int | None = None


@command("doc-index", short_help="Create docIndex.")
@option("--media-id", type=str, default=None, help="The mediaID value.")
@option(
    "--docs-in-collection",
    type=IntRange(1),
    default=10000,
    show_default=True,
    help="The maximum number of documents to put in each docCollection.",
)
@option(
    "--docs-in-media",
    type=IntRange(1),
    default=None,
    show_default=True,
    help="The maximum number of documents to put in each mediaID collection.",
)
@pass_context
def cmd_doc_index(ctx: Context, media_id: str | None, docs_in_collection: int, docs_in_media: int | None):
    """
    Create the docIndex.xml file from statutory files.

    To change the number of documents in each docCollection directory, use the --docs-in-collection option, ensuring
    the same number has been used to rearrange statutory files with the finalize doc-collections command.

    To change the number of documents in each mediaID collection, use the --docs-in-media option, ensuring that it is
    a multiple of the --docs-in-collection value to avoid splitting docCollections across multiple mediaIDs.
    """
    avid = get_avid(ctx)
    media_id = media_id or avid.path.name

    if m := match(r"^(AVID\.[A-Z]+\.[1-9]\d*)(?:\.[1-9]\d*)?$", media_id):
        media_id = m.group(1)
    else:
        raise BadParameter(f"{media_id!r} not in format AVID.ABCD.1234.1.", ctx, ctx_params(ctx)["media_id"])

    with open_database(ctx, avid) as database:
        _, log_stdout, _ = start_program(ctx, database, __version__, None, False, True, False)

        with ExceptionManager(BaseException) as exception:
            Event.from_command(ctx, "compiling").log(INFO, log_stdout)

            doc_index_base = database.create_table(
                DocIndexFile,
                "_doc_index_base",
                indices={"orig": ["original_uuid"], "parent": ["parent_uuid"]},
                ignore=["parent_doc_id"],
                temporary=True,
                exist_ok=True,
            )

            database.execute(f"""
                insert into {doc_index_base.name}
                select row_number() over (order by lower(fs.relative_path)) doc_id,
                       fs.uuid          as                                  uuid,
                       fs.relative_path as                                  relative_path,
                       fo.uuid          as                                  original_uuid,
                       fo.original_path as                                  original_path,
                       fo.parent        as                                  parent_uuid
                from files_statutory fs
                    join files_master fm on fm.uuid = fs.original_uuid
                    join files_original fo on fo.uuid = fm.original_uuid;
            """)

            doc_index = database.create_view(
                DocIndexFile,
                "_view_doc_index",
                f"""
                select di.*, dip.doc_id as parent_doc_id
                from {doc_index_base.name} di
                    left join {doc_index_base.name} dip on di.parent_uuid is not null and dip.original_uuid = di.parent_uuid
                order by di.doc_id
                """,
                temporary=True,
            )

            Event.from_command(ctx, "writing").log(INFO, log_stdout)

            with avid.dirs.indices.docIndex.open("w", encoding="utf-8") as fh:
                fh.write('<?xml version="1.0" encoding="utf-8"?>\n')
                # noinspection HttpUrlsUsage
                fh.write(
                    "<docIndex"
                    ' xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"'
                    ' xmlns="http://www.sa.dk/xmlns/diark/1.0"'
                    ' xsi:schemaLocation="http://www.sa.dk/xmlns/diark/1.0 ../Schemas/standard/docIndex.xsd">\n'
                )

                for file in doc_index.select():
                    doc_collection: int = ceil(file.doc_id / docs_in_collection)
                    doc_media_id: int = ceil(file.doc_id / docs_in_media) if docs_in_media else 1
                    fh.write("<doc>\n")
                    fh.write(f"  <dID>{file.doc_id}</dID>\n")
                    if file.parent_doc_id:
                        fh.write(f"  <pID>{file.parent_doc_id}</pID>\n")
                    fh.write(f"  <mID>{escape(media_id)}.{doc_media_id}</mID>\n")
                    fh.write(f"  <dCf>docCollection{doc_collection}</dCf>\n")
                    fh.write(f"  <oFn>{escape(file.original_path.name)}</oFn>\n")
                    fh.write(f"  <aFt>{file.relative_path.suffix[1:]}</aFt>\n")
                    if file.relative_path.suffix == ".gml":
                        fh.write(f"  <gmlXsd>{escape(file.relative_path.with_suffix('.xsd').name)}</gmlXsd>\n")
                    fh.write("</doc>\n")
                    Event.from_command(ctx, "document", (file.uuid, "statutory")).log(
                        INFO,
                        log_stdout,
                        dId=file.doc_id,
                        dCf=doc_collection,
                        mId=doc_media_id,
                    )

                fh.write("</docIndex>")

            Event.from_command(ctx, "cleanup").log(INFO, log_stdout)
            doc_index_base.drop(missing_ok=True)
            doc_index.drop(missing_ok=True)

        end_program(ctx, database, exception, exception.exception is None, log_stdout)
