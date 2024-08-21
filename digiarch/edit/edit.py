from click import group

from .action import group_action
from .lock import command_lock
from .remove import command_remove
from .rename import command_rename
from .rollback import command_rollback


@group("edit", no_args_is_help=True, short_help="Edit the database.")
def group_edit():
    """
    Edit the files' database.

    The ROOT argument in the edit subcommands is a folder that contains a _metadata/files.db database, not the
    _metadata folder itself.

    The ID arguments used in the edit subcommands are interpreted as a list of UUID's by default. This behaviour can
    be changed with the --puid, --path, --path-like, --checksum, and --warning options. If the --from-file option is
    used, each ID argument is interpreted as the path to a file containing a list of IDs (one per line, empty lines
    are ignored).

    Every edit subcommand requires a REASON argument that will be used in the database log to explain the reason behind
    the edit.
    """


# noinspection DuplicatedCode
group_edit.add_command(group_action, group_action.name)
group_edit.add_command(command_rename, command_rename.name)
group_edit.add_command(command_lock, command_lock.name)
group_edit.add_command(command_remove, command_remove.name)
group_edit.add_command(command_rollback, command_rollback.name)


group_edit.list_commands = lambda _ctx: list(group_edit.commands)
