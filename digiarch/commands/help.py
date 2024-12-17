from functools import reduce

from click import argument
from click import command
from click import Context
from click import Group
from click import Option
from click import option
from click import pass_context
from click import UsageError
from click.shell_completion import CompletionItem


def commands_completion(ctx: Context, param: Option, incomplete: str) -> list[CompletionItem]:
    try:
        # noinspection PyTypeChecker
        app: Group = ctx.find_root().command
        return [
            CompletionItem(n, help=c.short_help or c.get_short_help_str())
            for n, c in reduce(lambda a, c: a.commands[c], ctx.params.get(param.name, ctx.args), app).commands.items()
            if n.lower().startswith(incomplete.lower())
        ]
    except (KeyError, AttributeError):
        return []


@command("help", context_settings={"ignore_unknown_options": True})
@argument(
    "commands",
    nargs=-1,
    required=False,
    type=str,
    shell_complete=commands_completion,
)
@option("--database", expose_value=False, required=False, hidden=True)
@pass_context
def cmd_help(ctx: Context, commands: tuple[str, ...]):
    """Show the help for a command."""
    app = ctx.find_root().command
    cmds = [c for c in commands if not c.startswith("-")]

    try:
        cmd = reduce(lambda a, c: a.commands[c] if isinstance(a, Group) else a, cmds, app)
        print(cmd.get_help(app.make_context(cmd.name, cmds, ctx.parent)))
    except (KeyError, AttributeError):
        raise UsageError(f"No such command {' '.join(cmds)!r}.", ctx)
