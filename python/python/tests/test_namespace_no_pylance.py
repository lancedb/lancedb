# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Namespace operations must not require the optional ``pylance`` dependency.

The sync ``lancedb.connect()`` connection used to route namespace operations
through the Python ``lance_namespace`` client, whose ``dir`` implementation
lives in ``lance.namespace`` (shipped by the optional ``pylance`` extra). On an
install without that extra, even ``db.list_namespaces()`` failed, while the
async API worked because it goes straight to the native Rust connection.

The ``without_pylance`` fixture below simulates a missing ``pylance`` so these
tests fail on any environment when the native routing regresses. The ground
truth remains the "Test without pylance or pandas" CI job, which runs this file
with ``pylance`` and ``pandas`` actually uninstalled -- so nothing here may
import either at module scope.
"""

import sys
from importlib import import_module
from importlib.abc import MetaPathFinder

import lancedb
import pyarrow as pa
import pytest


def _is_lance(module_name: str) -> bool:
    # "lance_namespace" is a separate, non-optional package -- leave it alone.
    return module_name == "lance" or module_name.startswith("lance.")


class _BlockLanceImports(MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if _is_lance(fullname):
            raise ModuleNotFoundError(f"No module named {fullname!r}", name=fullname)
        return None


@pytest.fixture
def without_pylance(monkeypatch):
    """Make ``lance`` unimportable, as on an install without the pylance extra.

    Uninstalled means absent from ``sys.modules`` too, not merely unimportable:
    LanceDB has code that branches on ``"lance" in sys.modules``, so a fixture
    that poisons the entry instead of removing it would fail tests that a real
    install passes.
    """
    for name in list(sys.modules):
        if _is_lance(name):
            monkeypatch.delitem(sys.modules, name)
    monkeypatch.setattr(sys, "meta_path", [_BlockLanceImports(), *sys.meta_path])


def _schema() -> pa.Schema:
    return pa.schema([pa.field("id", pa.int64())])


def test_fixture_matches_an_uninstalled_pylance(without_pylance):
    """Guard the guard: the other tests are meaningless if lance stays importable."""
    assert "lance" not in sys.modules
    with pytest.raises(ModuleNotFoundError):
        import_module("lance.namespace")


def test_list_namespaces_on_sync_connection(tmp_path, without_pylance):
    """The original reproducer: this alone used to raise on a sync connection."""
    db = lancedb.connect(tmp_path)
    assert db.list_namespaces().namespaces == []


def test_sync_namespace_lifecycle(tmp_path, without_pylance):
    db = lancedb.connect(tmp_path)

    db.create_namespace(["child"])
    assert db.list_namespaces().namespaces == ["child"]
    assert db.list_namespaces(namespace_path=["child"]).namespaces == []
    db.describe_namespace(["child"])

    db.create_namespace(["child", "grandchild"])
    assert db.list_namespaces(namespace_path=["child"]).namespaces == ["grandchild"]

    db.drop_namespace(["child", "grandchild"])
    db.drop_namespace(["child"])
    assert db.list_namespaces().namespaces == []


def test_sync_namespaced_table_lifecycle(tmp_path, without_pylance):
    db = lancedb.connect(tmp_path)
    db.create_namespace(["child"])

    table = db.create_table("tbl", schema=_schema(), namespace_path=["child"])
    assert table.namespace == ["child"]
    table.add([{"id": 1}])

    assert db.list_tables(namespace_path=["child"]).tables == ["tbl"]
    assert db.list_tables().tables == []

    opened = db.open_table("tbl", namespace_path=["child"])
    assert opened.namespace == ["child"]
    assert opened.count_rows() == 1
    assert opened.search().limit(5).to_arrow().num_rows == 1

    db.drop_table("tbl", namespace_path=["child"])
    assert db.list_tables(namespace_path=["child"]).tables == []
    db.drop_namespace(["child"])


def test_sync_root_table_lifecycle(tmp_path, without_pylance):
    """Root-namespace tables share the namespace plumbing, so cover them too."""
    db = lancedb.connect(tmp_path)

    table = db.create_table("tbl", schema=_schema())
    table.add([{"id": 1}])

    assert db.table_names() == ["tbl"]
    assert "tbl" in db
    assert db["tbl"].count_rows() == 1

    db.drop_table("tbl")
    assert db.table_names() == []


@pytest.mark.asyncio
async def test_async_namespace_lifecycle(tmp_path, without_pylance):
    db = await lancedb.connect_async(tmp_path)

    await db.create_namespace(["child"])
    assert (await db.list_namespaces()).namespaces == ["child"]

    table = await db.create_table("tbl", schema=_schema(), namespace_path=["child"])
    await table.add([{"id": 1}])
    assert (await db.list_tables(namespace_path=["child"])).tables == ["tbl"]
    assert await table.count_rows() == 1

    await db.drop_table("tbl", namespace_path=["child"])
    await db.drop_namespace(["child"])
    assert (await db.list_namespaces()).namespaces == []


def test_namespace_client_still_requires_pylance(tmp_path, without_pylance):
    """``namespace_client()`` is the one namespace API that opts into pylance.

    It hands out a Python ``LanceNamespace``, so it cannot be served natively.
    Pinned here so the boundary stays explicit and the error stays actionable.
    """
    db = lancedb.connect(tmp_path)
    with pytest.raises(ValueError, match="lance.namespace.DirectoryNamespace"):
        db.namespace_client()
