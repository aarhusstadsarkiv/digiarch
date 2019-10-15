"""Module level documentation
Describe the module. Be precise.
End with a blank line before the last triple quote.

"""

# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------
import click

# -----------------------------------------------------------------------------
# Function Definitions
# -----------------------------------------------------------------------------


@click.group()
def cli():
    """A simple Command Line Tool."""


@cli.command("hello")
def hello():
    click.confirm("Do you want to continue?", abort=True)
    click.echo("Hi there.")
