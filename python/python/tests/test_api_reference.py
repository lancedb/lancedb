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

# Every module under `lancedb/` is checked except these, so a module added later is
# covered by default. Underscore-prefixed modules are skipped without being listed.
UNCHECKED_MODULES = {
    "lancedb.arrow",
    "lancedb.background_loop",
    "lancedb.common",
    "lancedb.conftest",
    "lancedb.dependencies",
    "lancedb.embeddings.gte_mlx_model",
    "lancedb.embeddings.utils",
    "lancedb.integrations",
    "lancedb.integrations.pyarrow",
    "lancedb.io",
    "lancedb.namespace_utils",
    "lancedb.remote.db",
    "lancedb.remote.errors",
    "lancedb.remote.table",
    "lancedb.rerankers.util",
    "lancedb.scannable",
    "lancedb.types",
    "lancedb.util",
}

# Public names deliberately kept out of the reference, keyed by where they are
# defined so one entry covers every alias. Each is a decision, not an oversight --
# add to this list only with a reason.
INTENTIONALLY_UNDOCUMENTED = {
    # Concrete implementations documented through their abstract base class.
    "lancedb.db.LanceDBConnection",
    "lancedb.remote.db.RemoteDBConnection",
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
    # Path-normalising helper that predates the reference page; undocumented and
    # has no docstring, but stays in `lancedb.__all__` for backwards compatibility.
    "lancedb.common.sanitize_uri",
    # Mixin that exists to be subclassed by the vector helpers, not used directly.
    "lancedb.pydantic.FixedSizeListMixin",
}

_DEFINITION_NODES = (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)


def _module_name(path: Path) -> str:
    parts = path.relative_to(PACKAGE_ROOT).with_suffix("").parts
    if parts and parts[-1] == "__init__":
        parts = parts[:-1]
    return ".".join(("lancedb", *parts))


@cache
def _trees() -> dict[str, ast.Module]:
    # A .pyi stub stands in for its compiled module (`lancedb._lancedb`).
    stubs = {p.with_suffix(".py") for p in PACKAGE_ROOT.rglob("*.pyi")}
    sources = [
        *(p for p in PACKAGE_ROOT.rglob("*.py") if p not in stubs),
        *PACKAGE_ROOT.rglob("*.pyi"),
    ]
    return {_module_name(p): ast.parse(p.read_text()) for p in sources}


def _checked_modules() -> list[str]:
    return [
        module
        for module in sorted(_trees())
        if module not in UNCHECKED_MODULES
        and not any(part.startswith("_") for part in module.split(".")[1:])
    ]


@cache
def _definitions() -> frozenset[str]:
    """Every ``module.Name`` that is a class or function defined in the package."""
    return frozenset(
        f"{module}.{node.name}"
        for module, tree in _trees().items()
        for node in tree.body
        if isinstance(node, _DEFINITION_NODES)
    )


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


def _public_names(module: str) -> set[str]:
    tree = _trees()[module]
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            getattr(target, "id", None) == "__all__" for target in node.targets
        ):
            return {element.value for element in node.value.elts}
    return _defined_names(module)


def _defined_names(module: str) -> set[str]:
    return {
        node.name
        for node in _trees()[module].body
        if isinstance(node, _DEFINITION_NODES) and not node.name.startswith("_")
    }


@cache
def _documented() -> frozenset[str]:
    """Every definition the reference page renders, by where it is defined."""
    rendered = set()
    for line in REFERENCE_PAGE.read_text().splitlines():
        if not line.startswith("::: "):
            continue
        target = line[4:].strip()
        if target in _trees():
            # A whole-module directive renders the names a module exports, plus any
            # public name defined in the module itself whether exported or not.
            paths = {
                f"{target}.{name}"
                for name in _public_names(target) | _defined_names(target)
            }
        else:
            paths = {target}
        rendered |= {d for d in map(_resolve, paths) if d is not None}
    return frozenset(rendered)


@pytest.mark.parametrize("module", _checked_modules())
def test_public_api_is_in_the_reference(module: str) -> None:
    missing = []
    for name in sorted(_public_names(module)):
        exported_as = f"{module}.{name}"
        definition = _resolve(exported_as)
        # Constants and type aliases (`lancedb.URI`, `__version__`) resolve to
        # nothing and have no place in a class/function reference.
        if definition is None:
            continue
        if definition in INTENTIONALLY_UNDOCUMENTED or definition in _documented():
            continue
        missing.append(exported_as)

    assert not missing, (
        f"{len(missing)} public name(s) in {module} are missing from "
        f"{REFERENCE_PAGE.relative_to(REPO_ROOT)}:\n  "
        + "\n  ".join(missing)
        + "\n\nAdd a `::: <path>` line to the matching section of that page, make the "
        "name private, or add it to INTENTIONALLY_UNDOCUMENTED in this test."
    )


def test_reference_has_no_stale_entries() -> None:
    """Every `::: lancedb...` target on the page resolves to real source."""
    stale = [
        line[4:].strip()
        for line in REFERENCE_PAGE.read_text().splitlines()
        if line.startswith("::: lancedb")
        and line[4:].strip() not in _trees()
        and _resolve(line[4:].strip()) is None
    ]
    assert not stale, (
        "Reference entries with no matching class or function:\n  " + "\n  ".join(stale)
    )


def test_intentionally_undocumented_is_accurate() -> None:
    """Keep the opt-out list from rotting into a set of false claims."""
    gone = sorted(INTENTIONALLY_UNDOCUMENTED - _definitions())
    assert not gone, "No longer defined, drop from the list:\n  " + "\n  ".join(gone)

    contradictory = sorted(INTENTIONALLY_UNDOCUMENTED & _documented())
    assert not contradictory, (
        "Listed as undocumented but the reference renders them:\n  "
        + "\n  ".join(contradictory)
    )

    # An entry earns its place only if some checked module exports a name that
    # resolves to it -- being defined in an unchecked module is not disqualifying,
    # since `lancedb` re-exports several of those.
    reachable = {
        _resolve(f"{module}.{name}")
        for module in _checked_modules()
        for name in _public_names(module)
    }
    unreachable = sorted(INTENTIONALLY_UNDOCUMENTED - reachable)
    assert not unreachable, (
        "Not exported by any checked module, so the entry does nothing:\n  "
        + "\n  ".join(unreachable)
    )
