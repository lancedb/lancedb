#  Copyright 2023 LanceDB Developers
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import click

from lancedb.utils import CONFIG


@click.group()
@click.version_option(help="LanceDB command line interface entry point")
def cli():
    "LanceDB command line interface"


diagnostics_help = """
Enable or disable LanceDB diagnostics. When enabled, LanceDB will send anonymous events
to help us improve LanceDB. These diagnostics are used only for error reporting and no
data is collected. You can find more about diagnosis on our docs:
https://lancedb.github.io/lancedb/cli_config/
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
