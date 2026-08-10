#!/usr/bin/env python3
"""Validate that every SDK uses the same published stable Lance release."""

from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from pathlib import Path

import tomllib

STABLE_VERSION = re.compile(r"[0-9]+\.[0-9]+\.[0-9]+")


def _stable_version(raw: str, *, dependency: str, exact: bool = False) -> str:
    value = raw.strip()
    if exact and not value.startswith("="):
        raise ValueError(f"Dependency '{dependency}' is not exact: {raw}")
    value = value.removeprefix("=").removeprefix("v")
    if STABLE_VERSION.fullmatch(value) is None:
        raise ValueError(f"Dependency '{dependency}' is not stable: {raw}")
    return value


def rust_lance_version(repo_root: Path) -> str:
    with (repo_root / "Cargo.toml").open("rb") as cargo_file:
        dependencies = tomllib.load(cargo_file)["workspace"]["dependencies"]

    versions: dict[str, str] = {}
    for name, dependency in dependencies.items():
        if name != "lance" and not name.startswith("lance-"):
            continue

        if isinstance(dependency, str):
            raw_version = dependency
        elif isinstance(dependency, dict):
            forbidden_sources = [
                source
                for source in ("git", "path", "branch", "rev", "tag")
                if source in dependency
            ]
            if forbidden_sources:
                joined = ", ".join(forbidden_sources)
                raise ValueError(
                    f"Dependency '{name}' uses unpublished source fields: {joined}"
                )
            raw_version = dependency.get("version")
            if raw_version is None:
                raise ValueError(f"Dependency '{name}' has no version")
        else:
            raise TypeError(f"Dependency '{name}' has an unexpected definition")

        versions[name] = _stable_version(raw_version, dependency=name, exact=True)

    if not versions:
        raise ValueError("No Rust Lance dependencies found")
    unique_versions = set(versions.values())
    if len(unique_versions) != 1:
        details = ", ".join(f"{name}={version}" for name, version in versions.items())
        raise ValueError(f"Rust Lance dependency versions do not match: {details}")
    return unique_versions.pop()


def python_lance_version(repo_root: Path) -> str:
    with (repo_root / "python" / "pyproject.toml").open("rb") as pyproject_file:
        pyproject = tomllib.load(pyproject_file)

    requirements = pyproject["project"]["optional-dependencies"]["tests"]
    pylance_requirements = [
        requirement for requirement in requirements if requirement.startswith("pylance")
    ]
    if len(pylance_requirements) != 1:
        raise ValueError(
            "Expected exactly one pylance requirement in the Python test dependencies"
        )

    requirement = pylance_requirements[0]
    prefix = "pylance=="
    if not requirement.startswith(prefix):
        raise ValueError(f"Python test dependency is not exact: {requirement}")
    return _stable_version(requirement[len(prefix) :], dependency="pylance")


def java_lance_version(repo_root: Path) -> str:
    pom_root = ET.parse(repo_root / "java" / "pom.xml").getroot()
    versions = [
        element.text
        for element in pom_root.iter()
        if element.tag.rsplit("}", 1)[-1] == "lance-core.version"
    ]
    if len(versions) != 1 or versions[0] is None:
        raise ValueError("Expected exactly one Java lance-core.version property")
    return _stable_version(versions[0], dependency="Java lance-core")


def validate(repo_root: Path) -> str:
    versions = {
        "Rust": rust_lance_version(repo_root),
        "Python": python_lance_version(repo_root),
        "Java": java_lance_version(repo_root),
    }
    if len(set(versions.values())) != 1:
        details = ", ".join(f"{sdk}={version}" for sdk, version in versions.items())
        raise ValueError(
            f"Lance dependency versions do not match across SDKs: {details}"
        )
    return versions["Rust"]


if __name__ == "__main__":
    version = validate(Path(__file__).resolve().parents[1])
    print(f"Validated published stable Lance v{version} across Rust, Python, and Java")
