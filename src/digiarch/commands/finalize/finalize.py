from click import group

from .doc_collections import cmd_doc_collections
from .doc_index import cmd_doc_index


@group("finalize", no_args_is_help=True, short_help="Finalize for delivery.")
def grp_finalize():
    """
    Perform the necessary opration to ready the AVID directory for delivery.

    \b
    The changes should be performed in the following order:
    * doc-collections
    * doc-index (TBA)
    * av-db (TBA)
    """  # noqa: D301


grp_finalize.add_command(cmd_doc_collections, cmd_doc_collections.name)
grp_finalize.add_command(cmd_doc_index, cmd_doc_index.name)

grp_finalize.list_commands = lambda _ctx: list(grp_finalize.commands)
