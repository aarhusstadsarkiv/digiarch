from pathlib import Path

import yaml
from acacore.models.reference_files import Action
from acacore.models.reference_files import CustomSignature
from acacore.reference_files import get_actions
from acacore.reference_files import get_custom_signatures
from acacore.utils.click import ctx_params
from click import argument
from click import BadParameter
from click import Context
from click import option
from click import Parameter
from click import Path as ClickPath
from pydantic import TypeAdapter


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
