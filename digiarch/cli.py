from acacore.__version__ import __version__ as __acacore_version__
from click import group
from click import version_option
from PIL import Image

from .__version__ import __version__
from .commands.cmd_import import cmd_import
from .commands.completions import cmd_completions
from .commands.extract.extract import cmd_extract
from .commands.identify import grp_identify
from .commands.init import cmd_init
from .commands.upgrade import cmd_upgrade

Image.MAX_IMAGE_PIXELS = int(50e3**2)


@group("digiarch", no_args_is_help=True)
@version_option(__version__, message=f"%(prog)s, version %(version)s\nacacore, version {__acacore_version__}")
def app():
    """Identify files and generate the database used by other Aarhus City Archives tools."""


# noinspection DuplicatedCode
app.add_command(cmd_init, cmd_init.name)
app.add_command(grp_identify, grp_identify.name)
app.add_command(cmd_extract, cmd_extract.name)
app.add_command(cmd_import, cmd_import.name)
app.add_command(cmd_upgrade, cmd_upgrade.name)
app.add_command(cmd_completions, cmd_completions.name)

app.list_commands = lambda _ctx: list(app.commands)
