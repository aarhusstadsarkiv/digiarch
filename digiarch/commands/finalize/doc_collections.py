from logging import INFO
from math import ceil
from pathlib import Path
from shutil import copy2

from acacore.models.event import Event
from acacore.utils.click import end_program
from acacore.utils.click import start_program
from acacore.utils.functions import rm_tree
from acacore.utils.helpers import ExceptionManager
from click import command
from click import Context
from click import IntRange
from click import option
from click import pass_context

from digiarch.__version__ import __version__
from digiarch.common import get_avid
from digiarch.common import open_database
from digiarch.common import option_dry_run


def safe_copy(src: Path, dst: Path):
    try:
        dst.parent.mkdir(parents=True, exist_ok=True)
        dst.unlink(missing_ok=True)
        copy2(src, dst)
    except:
        dst.unlink(missing_ok=True)
        raise


# TODO: handle GIS files
@command("doc-collections", short_help="Create docCollections.")
@option(
    "--docs-in-collection",
    type=IntRange(1),
    default=10000,
    show_default=True,
    help="The maximum number of documents to put in each docCollection.",
)
@option("--resume/--no-resume", is_flag=True, default=False, help="Resume a previously interrupted rearrangement.")
@option_dry_run()
@pass_context
def cmd_doc_collections(ctx: Context, docs_in_collection: int, resume: bool, dry_run: bool):
    """
    Rearrange files in Documents using docCollections.

    If the process is interrupted, all changes are rolled back, but the newly named files can be recovered using the
    --resume option when the command is run next. The option should only ever be used if NO other changes have occured
    to the files or the database. The default behaviour is to remove any leftover files and start the process anew.

    To change the number of documents in each docCollection directory, use the --docs-in-collection option.

    To see the changes without committing them, use the --dry-run option.
    """
    avid = get_avid(ctx)

    with open_database(ctx, avid) as database:
        _, log_stdout, _ = start_program(ctx, database, __version__, None, False, True, dry_run)

        with ExceptionManager() as exception:
            temp_dir: Path = avid.dirs.documents.joinpath("_metadata", "docCollections")
            temp_mv_dir: Path = avid.dirs.documents.joinpath("_metadata", "originals")

            if not dry_run and not resume and temp_dir.is_dir():
                Event.from_command(ctx, "reset:start").log(INFO, log_stdout)
                rm_tree(temp_dir)
                Event.from_command(ctx, "reset:end").log(INFO, log_stdout)

            statutory_files_temp = database.create_table(
                database.statutory_files.model,
                database.statutory_files.name + "_temp",
            )

            if not dry_run:
                rm_tree(temp_mv_dir)
                database.execute(f"drop table if exists {statutory_files_temp.name}")
                statutory_files_temp.create()
                database.commit()

            for doc_id, file in enumerate(database.statutory_files.select(order_by=[("relative_path", "asc")]), 1):
                collection_id: int = ceil(doc_id / docs_in_collection)
                new_path: Path = temp_dir.joinpath(f"docCollection{collection_id}", str(doc_id), f"1{file.suffix}")
                Event.from_command(ctx, "copy", (file.uuid, "statutory")).log(
                    INFO,
                    log_stdout,
                    path=file.relative_path,
                    new_path=new_path.relative_to(temp_dir),
                )
                if not dry_run:
                    if not new_path.is_file():
                        safe_copy(file.get_absolute_path(avid.path), new_path)
                    file.relative_path = Path(avid.dirs.documents.name, new_path.relative_to(temp_dir))
                    statutory_files_temp.insert(file)
                    if doc_id % docs_in_collection == 0:
                        database.commit()

            if not dry_run:
                database.commit()

                Event.from_command(ctx, "move:start").log(INFO, log_stdout)

                # Move Documents to _metadata/originals
                try:
                    temp_mv_dir.mkdir(parents=True, exist_ok=True)
                    for path in avid.dirs.documents.iterdir():
                        if temp_dir.is_relative_to(path):
                            continue
                        path.replace(temp_mv_dir.joinpath(path.relative_to(path.parent)))
                except:
                    # On error, move them back
                    for path in temp_mv_dir.iterdir():
                        path.replace(avid.dirs.documents.joinpath(path.relative_to(path.parent)))
                    raise

                # Move new to Documents
                try:
                    for path in temp_dir.iterdir():
                        path.replace(avid.dirs.documents.joinpath(path.relative_to(path.parent)))
                except:
                    # On error, empty Documents, move _metadata/originals back, and remove temporary table
                    for path in avid.dirs.documents.iterdir():
                        if temp_dir.is_relative_to(path):
                            continue
                        rm_tree(path)
                    for path in temp_mv_dir.iterdir():
                        path.replace(avid.dirs.documents.joinpath(path.relative_to(path.parent)))
                    database.execute(f"drop table if exists {statutory_files_temp.name}")
                    raise

                Event.from_command(ctx, "move:end").log(INFO, log_stdout)

                # Update entries in files_statutory with new data
                try:
                    Event.from_command(ctx, "update:start").log(INFO, log_stdout)
                    database.execute(
                        f"update {database.statutory_files.name} set relative_path = t.relative_path"
                        f" from {statutory_files_temp.name} t where {database.statutory_files.name}.uuid = t.uuid;"
                    )
                    Event.from_command(ctx, "update:end").log(INFO, log_stdout)
                except:
                    # On error, empty Documents, move _metadata/originals back, and roll back database changes
                    for path in avid.dirs.documents.iterdir():
                        if temp_dir.is_relative_to(path):
                            continue
                        rm_tree(path)
                    for path in temp_mv_dir.iterdir():
                        path.replace(avid.dirs.documents.joinpath(path.relative_to(path.parent)))
                    database.rollback()
                    raise
                finally:
                    # Always remove temporary table
                    database.execute(f"drop table if exists {statutory_files_temp.name}")

                Event.from_command(ctx, "cleanup:start").log(INFO, log_stdout)
                rm_tree(temp_dir)
                rm_tree(temp_mv_dir)
                if not next(avid.dirs.documents.joinpath("_metadata").iterdir(), None):
                    rm_tree(avid.dirs.documents.joinpath("_metadata"))
                database.commit()
                database.execute("vacuum")
                Event.from_command(ctx, "cleanup:end").log(INFO, log_stdout)

        end_program(ctx, database, exception, dry_run, log_stdout)
