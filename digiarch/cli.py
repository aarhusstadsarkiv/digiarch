from acacore.__version__ import __version__ as __acacore_version__
from click import group
from click import version_option
from PIL import Image

from .__version__ import __version__
from .commands.completions import cmd_completions
from .commands.upgrade import cmd_upgrade

Image.MAX_IMAGE_PIXELS = int(50e3**2)


@group("digiarch", no_args_is_help=True)
@version_option(__version__, message=f"%(prog)s, version %(version)s\nacacore, version {__acacore_version__}")
def app():
    """Identify files and generate the database used by other Aarhus City Archives tools."""


# noinspection DuplicatedCode
app.add_command(cmd_upgrade, cmd_upgrade.name)
app.add_command(cmd_completions, cmd_completions.name)

app.list_commands = lambda _ctx: list(app.commands)
