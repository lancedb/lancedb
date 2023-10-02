import click

from lancedb.utils import CONFIG

@click.group()
@click.version_option()
def cli():
    "LanceDB command line interface"

@cli.command()
@click.option('--enabled/--disabled', default=True, help='Enable or disable LanceDB Sync')
def sync(enabled):
    CONFIG.update({"sync": True if enabled else False})
    click.echo('LanceDB Sync is %s' % ('enabled' if enabled else 'disabled'))

@cli.command()
def config(help="Show current LanceDB configuration"):
    # TODO: pretty print as table with colors and formatting
    click.echo('Current LanceDB configuration:')
    cfg = CONFIG.copy()
    cfg.pop("uuid") # Don't show uuid as it is not configurable
    for item, amount in cfg.items():
        click.echo("{} ({})".format(item, amount))



    
