from acacore.__version__ import __version__ as __acacore_version__
from click import group
from click import version_option
from PIL import Image

from .__version__ import __version__
from .completions import command_completions
from .doctor import command_doctor
from .edit.edit import group_edit
from .extract.extract import command_extract
from .history import command_history
from .identify import command_identify
from .identify import command_reidentify
from .search import command_search
from .upgrade import command_upgrade

Image.MAX_IMAGE_PIXELS = int(50e3**2)


@group("digiarch", no_args_is_help=True)
@version_option(__version__, message=f"%(prog)s, version %(version)s\nacacore, version {__acacore_version__}")
def app():
    """Identify files and generate the database used by other Aarhus City Archives tools."""


# noinspection DuplicatedCode
app.add_command(command_identify, command_identify.name)
app.add_command(command_reidentify, command_reidentify.name)
app.add_command(command_extract, command_extract.name)
app.add_command(group_edit, group_edit.name)
app.add_command(command_search, command_search.name)
app.add_command(command_history, command_history.name)
app.add_command(command_doctor, command_doctor.name)
app.add_command(command_upgrade, command_upgrade.name)
app.add_command(command_completions, command_completions.name)

app.list_commands = lambda _ctx: list(app.commands)
