from click.testing import CliRunner

from lancedb.cli.cli import cli
from lancedb.utils import CONFIG


def test_entry():
    runner = CliRunner()
    result = runner.invoke(cli)
    assert result.exit_code == 0  # Main check
    assert "lancedb" in result.output.lower()  # lazy check


def test_diagnostics():
    runner = CliRunner()
    result = runner.invoke(cli, ["diagnostics", "--disabled"])
    assert result.exit_code == 0  # Main check
    assert not CONFIG["diagnostics"]

    result = runner.invoke(cli, ["diagnostics", "--enabled"])
    assert result.exit_code == 0  # Main check
    assert CONFIG["diagnostics"]


def test_config():
    runner = CliRunner()
    result = runner.invoke(cli, ["config"])
    assert result.exit_code == 0  # Main check
    cfg = CONFIG.copy()
    cfg.pop("uuid")
    for item in cfg:  # check for keys only as formatting is subject to change
        assert item in result.output
