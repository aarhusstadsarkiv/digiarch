from click import group

from .action import group_action
from .lock import command_lock
from .remove import command_remove
from .rename import command_rename
from .rollback import command_rollback


@group("edit", no_args_is_help=True, short_help="")
def group_edit():
    """Edit files' database."""


# noinspection DuplicatedCode
group_edit.add_command(group_action, group_action.name)
group_edit.add_command(command_rename, command_rename.name)
group_edit.add_command(command_lock, command_lock.name)
group_edit.add_command(command_remove, command_remove.name)
group_edit.add_command(command_rollback, command_rollback.name)


group_edit.list_commands = lambda _ctx: list(group_edit.commands)
