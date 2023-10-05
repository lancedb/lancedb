import click

from lancedb.utils import CONFIG


@click.group()
@click.version_option(help="LanceDB command line interface entry point")
def cli():
    "LanceDB command line interface"


diagnostics_help = """
Enable or disable LanceDB diagnostics. When enabled, LanceDB will send anonymous events to help us improve LanceDB.
These diagnostics are used only for error reporting and no data is collected. You can find more about diagnosis on
our docs: https://lancedb.github.io/lancedb/cli_config/
"""


@cli.command(help=diagnostics_help)
@click.option("--enabled/--disabled", default=True)
def diagnostics(enabled):
    CONFIG.update({"diagnostics": True if enabled else False})
    click.echo("LanceDB diagnostics is %s" % ("enabled" if enabled else "disabled"))


@cli.command(help="Show current LanceDB configuration")
def config():
    # TODO: pretty print as table with colors and formatting
    click.echo("Current LanceDB configuration:")
    cfg = CONFIG.copy()
    cfg.pop("uuid")  # Don't show uuid as it is not configurable
    for item, amount in cfg.items():
        click.echo("{} ({})".format(item, amount))
