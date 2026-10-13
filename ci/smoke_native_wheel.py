"""Exercise an installed macOS wheel, never a source-tree editable install."""

import argparse
import importlib.metadata
import platform
import subprocess
import sys
import tempfile
from pathlib import Path

import tomllib


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--architecture", choices=["x86_64", "arm64"], required=True)
    args = parser.parse_args()
    if sys.platform != "darwin" or platform.machine() != args.architecture:
        raise RuntimeError(
            "wheel smoke test must run on the requested macOS architecture"
        )

    import lancedb
    import lancedb._lancedb as native
    import pyarrow as pa
    from packaging.version import Version

    repo = Path(__file__).resolve().parents[1]
    expected = tomllib.loads((repo / "python" / "Cargo.toml").read_text())["package"][
        "version"
    ]
    assert Version(importlib.metadata.version("lancedb")) == Version(expected)
    assert not Path(native.__file__).resolve().is_relative_to(repo)
    architectures = subprocess.check_output(
        ["lipo", "-archs", native.__file__], text=True
    ).strip()
    assert architectures == args.architecture, architectures

    with tempfile.TemporaryDirectory() as root:
        db = lancedb.connect(root)
        schema = pa.schema([("id", pa.string()), ("vector", pa.list_(pa.float32(), 3))])
        table = db.create_table("memories", schema=schema)
        table.add([{"id": "first", "vector": [1.0, 0.0, 0.0]}])
        table.add_columns({"scopeKey": "cast('' as string)"})
        table.add([{"id": "second", "vector": [0.0, 1.0, 0.0], "scopeKey": "private"}])
        assert table.count_rows() == 2
        assert table.search([1.0, 0.0, 0.0]).limit(1).to_list()[0]["id"] == "first"
        assert (
            table.search([0.0, 1.0, 0.0])
            .where("scopeKey = 'private'")
            .limit(1)
            .to_list()[0]["id"]
            == "second"
        )
        table.update(where="id = 'first'", values={"scopeKey": "updated"})
        reopened = lancedb.connect(root).open_table("memories")
        assert reopened.count_rows("scopeKey = 'updated'") == 1
        assert reopened.schema.field("vector").type.list_size == 3
        reopened.delete("id = 'second'")
        assert reopened.count_rows() == 1
    print(f"PASS: installed LanceDB {expected} on native {args.architecture}")


if __name__ == "__main__":
    main()
