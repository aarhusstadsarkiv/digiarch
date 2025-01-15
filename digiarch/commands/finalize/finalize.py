from click import group


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


grp_finalize.list_commands = lambda _ctx: list(grp_finalize.commands)
