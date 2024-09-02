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

    \b
    The QUERY argument uses a simple search syntax.
    @<field> will match a specific field, the following are supported: uuid,
    checksum, puid, relative_path, action, warning, processed, lock.
    @null and @notnull will match columns with null and not null values respectively.
    @true and @false will match columns with true and false values respectively.
    @like toggles LIKE syntax for the values following it in the same column.
    @file toggles file reading for the values following it in the same column: each
    value will be considered as a file path and values will be read from the lines
    in the given file (@null, @notnull, @true, and @false in files are not supported).
    Changing to a new @<field> resets like and file toggles. Values for the same
    column will be matched with OR logic, while values from different columns will
    be matched with AND logic.

    Every edit subcommand requires a REASON argument that will be used in the database log to explain the reason behind
    the edit.

    \b
    Query Examples
    --------------

    @uuid @file uuids.txt @warning @notnull = (uuid = ? or uuid = ? or uuid = ?) and (warning is not null)

    @relative_path @like %.pdf @lock @true = (relative_path like ?) and (lock is true)

    @action convert @relative_path @like %.pdf %.msg = (action = ?) and (relative_path like ? or relative_path like ?)
    """  # noqa: D301


# noinspection DuplicatedCode
group_edit.add_command(group_action, group_action.name)
group_edit.add_command(command_rename, command_rename.name)
group_edit.add_command(command_lock, command_lock.name)
group_edit.add_command(command_remove, command_remove.name)
group_edit.add_command(command_rollback, command_rollback.name)


group_edit.list_commands = lambda _ctx: list(group_edit.commands)
