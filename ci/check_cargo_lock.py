"""Fail if pruning Cargo.lock changes pins or adds dependency edges.

Removing workspace members can disable optional dependency edges even when
their target packages remain in use elsewhere in the packaged workspace.

Usage: python ci/check_cargo_lock.py <lock-before> <lock-after>
"""

import sys
import tomllib
from pathlib import Path


def key(package: dict) -> tuple:
    return tuple(package.get(k) for k in ("name", "version", "source", "checksum"))


def graph(lock: dict) -> dict[tuple, set[tuple]]:
    """Map each package to the packages its dependency strings resolve to."""
    by_name = {}
    for p in lock["package"]:
        by_name.setdefault(p["name"], []).append(p)

    def resolve(dep: str) -> tuple:
        # Cargo writes "name", "name version" or "name version (source)".
        name, *rest = dep.split(" ", 2)
        (match,) = [
            p
            for p in by_name[name]
            if (not rest or p["version"] == rest[0])
            and (len(rest) < 2 or rest[1] == f"({p.get('source')})")
        ]
        return key(match)

    return {
        key(p): {resolve(d) for d in p.get("dependencies", [])} for p in lock["package"]
    }


def main() -> None:
    before, after = (tomllib.loads(Path(p).read_text()) for p in sys.argv[1:])
    errors = []
    for field in (before.keys() | after.keys()) - {"package"}:
        if before.get(field) != after.get(field):
            errors.append(f"top-level {field!r} changed")
    edges_before, edges_after = graph(before), graph(after)
    removed = edges_before.keys() - edges_after.keys()
    for package, deps in edges_after.items():
        if package not in edges_before:
            errors.append(f"added or changed {package}")
        elif not deps <= edges_before[package]:
            errors.append(f"dependencies of {package[:2]} changed")
    if errors:
        sys.exit("error: the prune changed Cargo.lock:\n" + "\n".join(errors))
    print(f"Cargo.lock prune only removed {len(removed)} packages")


if __name__ == "__main__":
    main()
