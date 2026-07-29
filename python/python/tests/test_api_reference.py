# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Guard against the Python API reference drifting from the public API.

`docs/src/python/python.md` is the whole Python API reference and is maintained
by hand, so a new public class or function is simply absent from the docs until
someone remembers to list it. This test fails when that happens.

Everything is derived from the source with `ast` rather than by importing, both
because the docs are built the same way (griffe reads the sources) and because
some modules need optional dependencies that CI does not install.
"""

import ast
from functools import cache
from pathlib import Path
from typing import Optional

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
PACKAGE_ROOT = REPO_ROOT / "python" / "python" / "lancedb"
REFERENCE_PAGE = REPO_ROOT / "docs" / "src" / "python" / "python.md"

pytestmark = pytest.mark.skipif(
    not REFERENCE_PAGE.exists(),
    reason="needs a source checkout; docs/ is not shipped in the wheel",
)

# Modules whose public surface belongs in the reference. Modules absent from this
# list (internal helpers such as `lancedb.util` or `lancedb._blob`) are not checked.
CHECKED_MODULES = [
    "lancedb",
    "lancedb.context",
    "lancedb.db",
    "lancedb.embeddings",
    "lancedb.exceptions",
    "lancedb.expr",
    "lancedb.index",
    "lancedb.merge",
    "lancedb.namespace",
    "lancedb.otel",
    "lancedb.permutation",
    "lancedb.pydantic",
    "lancedb.query",
    "lancedb.remote",
    "lancedb.rerankers",
    "lancedb.schema",
    "lancedb.streaming",
    "lancedb.table",
]

# Public names deliberately kept out of the reference. Each entry is a decision,
# not an oversight -- add to this list only with a reason.
INTENTIONALLY_UNDOCUMENTED = {
    # Concrete implementations documented through their abstract base class.
    "lancedb.LanceDBConnection",
    "lancedb.RemoteDBConnection",
    "lancedb.db.LanceDBConnection",
    "lancedb.table.LanceTable",
    # Base classes folded into the concrete query classes by mkdocstrings'
    # `inherited_members` option.
    "lancedb.query.AsyncQueryBase",
    "lancedb.query.AsyncStandardQuery",
    "lancedb.query.AsyncVectorQueryBase",
    "lancedb.query.BaseQueryBuilder",
    # Wire and plumbing types users do not construct directly.
    "lancedb.query.ColumnOrdering",
    "lancedb.query.FullTextQueryType",
    "lancedb.query.FullTextSearchQuery",
    "lancedb.query.ensure_vector_query",
    # Path-normalising helper that predates the reference page; undocumented and
    # has no docstring, but stays in `lancedb.__all__` for backwards compatibility.
    "lancedb.common.sanitize_uri",
    # Internal helpers that happen to lack a leading underscore.
    "lancedb.permutation.Permutations",
    "lancedb.permutation.Transforms",
    "lancedb.pydantic.FixedSizeListMixin",
    "lancedb.pydantic.get_extras",
    "lancedb.pydantic.is_nullable",
    "lancedb.pydantic.model_to_dict",
    "lancedb.schema.blob_column_paths",
    "lancedb.schema.blob_v2_column_paths",
    "lancedb.schema.is_blob_like_field",
    "lancedb.schema.is_blob_v2_field",
    "lancedb.schema.schema_has_blob_field",
    "lancedb.table.has_nan_values",
    "lancedb.table.sanitize_create_table",
}

_DEFINITION_NODES = (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)


def _module_name(path: Path) -> str:
    parts = path.relative_to(PACKAGE_ROOT).with_suffix("").parts
    if parts and parts[-1] == "__init__":
        parts = parts[:-1]
    return ".".join(("lancedb", *parts))


def _source_files() -> list[Path]:
    # A .pyi stub stands in for its compiled module (`lancedb._lancedb`).
    stubs = {p.with_suffix(".py") for p in PACKAGE_ROOT.rglob("*.pyi")}
    return [
        *(p for p in PACKAGE_ROOT.rglob("*.py") if p not in stubs),
        *PACKAGE_ROOT.rglob("*.pyi"),
    ]


@cache
def _trees() -> dict[str, ast.Module]:
    return {_module_name(p): ast.parse(p.read_text()) for p in _source_files()}


@cache
def _definitions() -> set[str]:
    """Every ``module.Name`` that is a class or function defined in the package."""
    return {
        f"{module}.{node.name}"
        for module, tree in _trees().items()
        for node in tree.body
        if isinstance(node, _DEFINITION_NODES)
    }


@cache
def _reexports() -> dict[str, str]:
    """Map each re-exported path to the path it was imported from.

    `lancedb/__init__.py` doing `from .table import Table` yields
    ``lancedb.Table -> lancedb.table.Table``.
    """
    edges: dict[str, str] = {}
    for module, tree in _trees().items():
        for node in ast.walk(tree):
            if not isinstance(node, ast.ImportFrom) or node.level == 0:
                continue
            base = module if _is_package(module) else module.rsplit(".", 1)[0]
            for _ in range(node.level - 1):
                base = base.rsplit(".", 1)[0]
            source = f"{base}.{node.module}" if node.module else base
            for alias in node.names:
                edges[f"{module}.{alias.asname or alias.name}"] = (
                    f"{source}.{alias.name}"
                )
    return edges


def _is_package(module: str) -> bool:
    relative = module.split(".")[1:]
    return (PACKAGE_ROOT.joinpath(*relative) / "__init__.py").exists()


def _resolve(path: str) -> Optional[str]:
    """Follow re-exports to where a name is actually defined."""
    seen = set()
    while path not in _definitions():
        if path in seen or path not in _reexports():
            return None
        seen.add(path)
        path = _reexports()[path]
    return path


@cache
def _paths_to(definition: str) -> frozenset[str]:
    """Every importable path that reaches a definition, including its own.

    `lancedb.schema.blob` is also reachable as `lancedb.blob`, and the reference
    may legitimately document it under either name.
    """
    return frozenset(
        {definition, *(p for p in _reexports() if _resolve(p) == definition)}
    )


def _public_names(module: str) -> tuple[set[str], bool]:
    """Public names of a module, and whether they came from ``__all__``."""
    tree = _trees()[module]
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            getattr(target, "id", None) == "__all__" for target in node.targets
        ):
            return {element.value for element in node.value.elts}, True
    return {
        node.name
        for node in tree.body
        if isinstance(node, _DEFINITION_NODES) and not node.name.startswith("_")
    }, False


@cache
def _documented() -> tuple[frozenset[str], frozenset[str]]:
    """Individual symbols, and whole modules, rendered by the reference page."""
    symbols, modules = set(), set()
    for line in REFERENCE_PAGE.read_text().splitlines():
        if not line.startswith("::: "):
            continue
        target = line[4:].strip()
        (modules if target in _trees() else symbols).add(target)
    return frozenset(symbols), frozenset(modules)


@pytest.mark.parametrize("module", CHECKED_MODULES)
def test_public_api_is_in_the_reference(module: str) -> None:
    symbols, rendered_modules = _documented()
    names, from_dunder_all = _public_names(module)

    missing = []
    for name in sorted(names):
        exported_as = f"{module}.{name}"
        if exported_as in INTENTIONALLY_UNDOCUMENTED:
            continue
        defined_at = _resolve(exported_as)
        # Constants and type aliases (`lancedb.URI`, `__version__`) resolve to
        # nothing and have no place in a class/function reference.
        if defined_at is None or defined_at in INTENTIONALLY_UNDOCUMENTED:
            continue
        # A whole-module directive renders everything the module exports, but for
        # a re-export package only the names listed in `__all__`.
        rendered_wholesale = module in rendered_modules and (
            from_dunder_all or defined_at == exported_as
        )
        if rendered_wholesale or symbols & _paths_to(defined_at):
            continue
        missing.append(exported_as)

    assert not missing, (
        f"{len(missing)} public name(s) in {module} are missing from "
        f"{REFERENCE_PAGE.relative_to(REPO_ROOT)}:\n  "
        + "\n  ".join(missing)
        + "\n\nAdd a `::: <path>` line to the matching section of that page, or "
        "add the name to INTENTIONALLY_UNDOCUMENTED in this test with a reason."
    )


def test_reference_has_no_stale_entries() -> None:
    """Every `::: lancedb...` target on the page resolves to real source."""
    symbols, modules = _documented()
    stale = [
        target
        for target in sorted(symbols)
        if _resolve(target) is None and target not in _definitions()
    ]
    assert not stale, (
        "Reference entries with no matching class or function:\n  " + "\n  ".join(stale)
    )
    assert modules, "Expected the page to render at least one module wholesale"
