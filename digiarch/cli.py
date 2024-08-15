from acacore.__version__ import __version__ as __acacore_version__
from click import group
from click import version_option

from .__version__ import __version__
from .doctor import command_doctor
from .edit.edit import group_edit
from .history import command_history
from .identify.identify import command_identify
from .identify.identify import command_reidentify
from .upgrade import command_upgrade


@group("digiarch", no_args_is_help=True)
@version_option(__version__, message=f"%(prog)s, version %(version)s\nacacore, version {__acacore_version__}")
def app():
    """Generate and operate on the files' database used by other Aarhus Stadsarkiv tools."""


# noinspection DuplicatedCode
app.add_command(command_identify, command_identify.name)
app.add_command(command_reidentify, command_reidentify.name)
app.add_command(group_edit, group_edit.name)
app.add_command(command_history, command_history.name)
app.add_command(command_doctor, command_doctor.name)
app.add_command(command_upgrade, command_upgrade.name)

app.list_commands = lambda _ctx: list(app.commands)
