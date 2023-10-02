import click

from lancedb.utils import CONFIG


@click.group()
@click.version_option(help="LanceDB command line interface entry point")
def cli():
    "LanceDB command line interface"


@cli.command(help="Enable or disable LanceDB Sync")
@click.option("--enabled/--disabled", default=True)
def sync(enabled):
    CONFIG.update({"sync": True if enabled else False})
    click.echo("LanceDB Sync is %s" % ("enabled" if enabled else "disabled"))


@cli.command(help="Show current LanceDB configuration")
def config():
    # TODO: pretty print as table with colors and formatting
    click.echo("Current LanceDB configuration:")
    cfg = CONFIG.copy()
    cfg.pop("uuid")  # Don't show uuid as it is not configurable
    for item, amount in cfg.items():
        click.echo("{} ({})".format(item, amount))
