from datetime import datetime
from logging import ERROR
from logging import INFO
from logging import Logger
from pathlib import Path
from re import compile as re_compile
from re import Pattern
from sqlite3 import DatabaseError
from sys import stdout
from traceback import format_tb
from typing import Any
from typing import Callable

import yaml
from acacore.database import FileDB
from acacore.database.upgrade import is_latest
from acacore.models.history import HistoryEntry
from acacore.models.reference_files import Action
from acacore.models.reference_files import CustomSignature
from acacore.reference_files import get_actions
from acacore.reference_files import get_custom_signatures
from acacore.utils.helpers import ExceptionManager
from acacore.utils.log import setup_logger
from click import argument
from click import BadParameter
from click import Command
from click import Context
from click import option
from click import Parameter
from click import Path as ClickPath
from pydantic import TypeAdapter

from .__version__ import __version__


def ctx_params(ctx: Context) -> dict[str, Parameter]:
    return {p.name: p for p in ctx.command.params}


def param_regex(pattern: str, flags: int = 0):
    compiled_pattern: Pattern = re_compile(pattern, flags)

    def callback(ctx: Context, param: Parameter, value: str | tuple[str, ...] | None) -> str | tuple[str, ...] | None:
        if value is None:
            return value
        elif isinstance(value, str) and not compiled_pattern.match(value):  # noqa: SIM114
            raise BadParameter(f"does not match {pattern!r}", ctx, param)
        elif isinstance(value, tuple) and any(not compiled_pattern.match(v) for v in value):
            raise BadParameter(f"does not match {pattern!r}", ctx, param)
        return value

    return callback


def copy_params(command: Command) -> Callable[[Command], Command]:
    def decorator(command2: Command) -> Command:
        command2.params.extend(command.params.copy())
        return command2

    return decorator


def argument_root(exists: bool):
    if exists:

        def _callback(ctx: Context, param: Parameter, value: str):
            if not (path := Path(value)).joinpath("_metadata", "files.db").is_file():
                raise BadParameter(f"No _metadata/files.db present in {value!r}.", ctx, param)
            return path

        return argument(
            "root",
            nargs=1,
            type=ClickPath(exists=True, file_okay=False, writable=True, resolve_path=True),
            callback=_callback,
        )
    else:
        return argument(
            "root",
            nargs=1,
            type=ClickPath(file_okay=False, writable=True, resolve_path=True),
            callback=lambda _ctx, _param, value: Path(value),
        )


def option_dry_run():
    return option("--dry-run", is_flag=True, default=False, help="Show changes without committing them.")


def docstring_format(**kwargs: Any) -> Callable[[Callable], Callable]:  # noqa: ANN401
    def decorator(func: Callable) -> Callable:
        func.__doc__ = (func.__doc__ or "").format(**kwargs)
        return func

    return decorator


def check_database_version(ctx: Context, param: Parameter, path: Path):
    if not path.is_file():
        return
    with FileDB(path, check_version=False) as db:
        try:
            is_latest(db, raise_on_difference=True)
        except DatabaseError as err:
            raise BadParameter(err.args[0], ctx, param)


def fetch_actions(ctx: Context, parameter_name: str, file: Path | None) -> dict[str, Action]:
    if file:
        try:
            with file.open() as fh:
                return TypeAdapter(dict[str, Action]).validate_python(yaml.load(fh, yaml.Loader))
        except BaseException:
            raise BadParameter("Invalid actions file.", ctx, ctx_params(ctx)[parameter_name])

    try:
        return get_actions()
    except BaseException as err:
        raise BadParameter(
            f"Cannot download actions. {err.args[0] if err.args else ''}", ctx, ctx_params(ctx)[parameter_name]
        )


def fetch_custom_signatures(ctx: Context, parameter_name: str, file: Path | None) -> list[CustomSignature]:
    if file:
        try:
            with file.open() as fh:
                return TypeAdapter(list[CustomSignature]).validate_python(yaml.load(fh, yaml.Loader))
        except BaseException:
            raise BadParameter("Invalid custom signatures file.", ctx, ctx_params(ctx)[parameter_name])

    try:
        return get_custom_signatures()
    except BaseException as err:
        raise BadParameter(
            f"Cannot download actions. {err.args[0] if err.args else ''}", ctx, ctx_params(ctx)[parameter_name]
        )


def start_program(
    ctx: Context,
    database: FileDB,
    time: datetime | None = None,
    log_file: bool = True,
    log_stdout: bool = True,
    dry_run: bool = False,
) -> tuple[Logger | None, Logger | None]:
    prog: str = ctx.find_root().command.name
    log_file: Logger | None = (
        setup_logger(f"{prog}_file", files=[database.path.parent / f"{prog}.log"]) if log_file else None
    )
    log_stdout: Logger | None = setup_logger(f"{prog}_stdout", streams=[stdout]) if log_stdout else None
    program_start: HistoryEntry = HistoryEntry.command_history(
        ctx,
        "start",
        data={"version": __version__},
        add_params_to_data=True,
        time=time,
    )

    if not dry_run:
        database.history.insert(program_start)

    if log_file:
        program_start.log(INFO, log_file)
    if log_stdout:
        program_start.log(INFO, log_stdout, show_args=False)

    return log_file, log_stdout


def end_program(
    ctx: Context,
    database: FileDB,
    exception: ExceptionManager,
    dry_run: bool = False,
    *loggers: Logger | None,
):
    program_end: HistoryEntry = HistoryEntry.command_history(
        ctx,
        "end",
        data=repr(exception.exception) if exception.exception else None,
        reason="".join(format_tb(exception.traceback)) if exception.traceback else None,
    )

    for logger in (log for log in loggers if log):
        program_end.log(ERROR if exception.exception else INFO, logger)

    if not dry_run:
        database.history.insert(program_end)
        database.commit()
