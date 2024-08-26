from typing import ClassVar
from typing import Type

from click import argument
from click import BadParameter
from click import Choice
from click import command
from click import Context
from click import Parameter
from click import pass_context
from click.shell_completion import BashComplete
from click.shell_completion import CompletionItem
from click.shell_completion import FishComplete
from click.shell_completion import get_completion_class
from click.shell_completion import ShellComplete
from click.shell_completion import ZshComplete

from digiarch.common import ctx_params
from digiarch.common import docstring_format


class CompleteChoice(Choice):
    completion_items: ClassVar[list[CompletionItem]] = []

    def __init__(self) -> None:
        super().__init__([c.value for c in self.completion_items], False)

    def shell_complete(self, _ctx: Context, _param: Parameter, incomplete: str) -> list[CompletionItem]:
        return [i for i in self.completion_items if i.value.lower().startswith(incomplete.lower())]


class ShellChoice(CompleteChoice):
    completion_items: ClassVar[list[CompletionItem]] = [
        CompletionItem(BashComplete.name, help="Bourne Again Shell"),
        CompletionItem(FishComplete.name, help="Friendly Interactive Shell"),
        CompletionItem(ZshComplete.name, help="Z shell"),
    ]


@command("completions", no_args_is_help=True, short_help="Generate shell completions.")
@argument("shell", type=ShellChoice(), callback=lambda _c, _p, v: get_completion_class(v))
@pass_context
@docstring_format(shells="\n    ".join(f"* {s.value}\t{s.help}" for s in ShellChoice.completion_items))
def command_completions(ctx: Context, shell: Type[ShellComplete] | None):
    """
    Generate tab-completion scripts for your shell. The generated completion must be saved in the correct location for
    it to be recognized and used by the shell.

    \b
    Supported shells are:
    {shells}
    """

    if not shell:
        raise BadParameter("shell not found.", ctx, ctx_params(ctx)["shell"])

    # noinspection PyTypeChecker
    print(
        shell(
            (prog := ctx.find_root()).command,
            {},
            prog.info_name,
            f"_{prog.info_name.replace('-', '_').upper()}_COMPLETE",
        ).source()
    )
