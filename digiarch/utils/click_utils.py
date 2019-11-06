"""Utilities for the click portion of Digital Archive.

"""

# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------
import click

# -----------------------------------------------------------------------------
# Function Definitions
# -----------------------------------------------------------------------------


def click_ok(message: str) -> None:
    """Function for returning nice okay message.

    Parameters
    ----------
    message : str
        The message to return using `click.echo`

    """
    click.secho("🗸 ", fg="green", nl=False)
    click.echo(message)


def click_warn(message: str) -> None:
    """Function for returning nice warning message.

    Parameters
    ----------
    message : str
        The message to return using `click.echo`

    """
    click.secho("⚠️ ", fg="red", nl=False)
    click.echo(message)
