from functools import reduce
from logging import INFO
from logging import Logger
from pathlib import Path

from acacore.database import FileDB
from acacore.models.history import HistoryEntry
from acacore.utils.helpers import ExceptionManager
from click import Choice
from click import command
from click import Context
from click import option
from click import pass_context

from digiarch.common import argument_root
from digiarch.common import check_database_version
from digiarch.common import ctx_params
from digiarch.common import end_program
from digiarch.common import option_dry_run
from digiarch.common import start_program


def sanitize_paths(ctx: Context, database: FileDB, root: Path, dry_run: bool, *loggers: Logger | None):
    invalid_characters: str = r'\?%*|"<>,:;=+[]!@' + bytes(range(20)).decode("ascii") + "\x7f"
    for file in database.files.select(
        where=" or ".join("instr(relative_path, ?) != 0" for _ in invalid_characters),
        parameters=list(invalid_characters),
    ):
        file.root = root
        old_path: Path = file.relative_path
        new_path: Path = Path(
            *[reduce(lambda acc, cur: acc.replace(cur, "_"), invalid_characters, p) for p in old_path.parts]
        )

        while file.root.joinpath(new_path).exists():
            new_path = new_path.with_name("_" + new_path.name)

        if not dry_run:
            file.root.joinpath(new_path).parent.mkdir(parents=True, exist_ok=True)
            file.get_absolute_path().rename(file.root / new_path)
            file.relative_path = new_path
            try:
                database.files.update(file, {"uuid": file.uuid})
            except BaseException:
                file.get_absolute_path().rename(file.root / old_path)
                raise

        event: HistoryEntry = HistoryEntry.command_history(
            ctx,
            "sanitize-path",
            file.uuid,
            [str(file.relative_path), str(new_path)],
        )
        if not dry_run:
            database.history.insert(event)

        event.log(INFO, *(log for log in loggers if log))


def deduplicate_extensions(ctx: Context, database: FileDB, root: Path, dry_run: bool, *loggers: Logger | None):
    for file in database.files.select(
        where="instr(reverse(relative_path), '.') != 0"
        " and relative_path like '%' ||"
        " substr(relative_path, length(relative_path) - instr(reverse(relative_path), '.') + 1) ||"
        " substr(relative_path, length(relative_path) - instr(reverse(relative_path), '.') + 1)"
    ):
        old_suffixes: list[str] = [s.lower() for s in file.suffixes.split(".") if s]
        new_suffixes: list[str] = [s.lower() for s in old_suffixes]
        # Deduplicate suffixes
        new_suffixes = sorted(set(new_suffixes), key=new_suffixes.index)
        # Restore original letter case
        new_suffixes = [next(s2 for s2 in old_suffixes if s2.lower() == s) for s in new_suffixes]
        old_name: str = file.name
        new_name: str = old_name.removesuffix("." + ".".join(old_suffixes)) + "." + ".".join(new_suffixes)

        if not new_name or old_name == new_name:
            return

        file.root = root
        old_path: Path = file.get_absolute_path()
        new_path: Path = file.get_absolute_path().with_name(new_name)

        if new_path.exists():
            HistoryEntry.command_history(
                ctx,
                "deduplicate-extensions.skip",
                file.uuid,
                [old_path.relative_to(root), new_path.relative_to(root)],
                reason="New path already exists.",
            ).log(INFO, *loggers)
            return

        if not dry_run:
            old_path.rename(new_path)
            try:
                file.name = new_name
                database.files.update(file, {"uuid": file.uuid})
            except BaseException:
                new_path.rename(old_path)
                raise

        event: HistoryEntry = HistoryEntry.command_history(
            ctx,
            "deduplicate-extensions",
            file.uuid,
            [old_path.relative_to(root), new_path.relative_to(root)],
        )
        if not dry_run:
            database.history.insert(event)

        event.log(INFO, *(log for log in loggers if log))


@command("doctor", no_args_is_help=True, short_help="Inspect the database.")
@argument_root(True)
@option(
    "--fix",
    type=Choice(["paths", "extensions"]),
    multiple=True,
    help="Specify which fixes to apply.",
)
@option_dry_run()
@pass_context
def command_doctor(ctx: Context, root: Path, fix: tuple[str, ...], dry_run: bool):
    """
    Inspect the database for common issues.

    \b
    The current fixes will be applied:
    * Path sanitizing (paths): paths containing any invalid characters (\\?%*|"<>,:;=+[]!@) will be renamed with those
        characters removed
    * Duplicated extensions (extensions): paths ending with duplicated extensions will be rewritten to remove
        duplicated extensions and leave only one

    To see the changes without committing them, use the --dry-run option.
    """  # noqa: D301
    check_database_version(ctx, ctx_params(ctx)["root"], (db_path := root / "_metadata" / "files.db"))

    with FileDB(db_path) as database:
        log_file, log_stdout = start_program(ctx, database, None, not dry_run, True, False)

        with ExceptionManager(BaseException) as exception:
            if not fix or "paths" in fix:
                HistoryEntry.command_history(ctx, "sanitize-paths.start").log(INFO, log_stdout)
                sanitize_paths(ctx, database, root, dry_run, log_stdout)
            if not fix or "extensions" in fix:
                HistoryEntry.command_history(ctx, "deduplicate-extensions.start").log(INFO, log_stdout)
                deduplicate_extensions(ctx, database, root, dry_run, log_stdout)

        end_program(ctx, database, exception, dry_run, log_file, log_stdout)
