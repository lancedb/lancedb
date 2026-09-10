# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import shutil
from pathlib import Path

import lancedb
import pytest
import pytest_asyncio
from lancedb.db import AsyncConnection, DBConnection


class DatabasePathFactory:
    """Create per-test database directories and ensure they get removed."""

    def __init__(self, base_dir: Path) -> None:
        self._base_dir = base_dir
        self._created: list[Path] = []

    def __call__(self, name: str) -> Path:
        safe_name = name.replace("/", "_")
        path = self._base_dir / safe_name
        if path.exists():
            shutil.rmtree(path, ignore_errors=True)
        path.mkdir(parents=True, exist_ok=True)
        self._created.append(path)
        return path

    def cleanup(self) -> None:
        for path in reversed(self._created):
            shutil.rmtree(path, ignore_errors=True)
        self._created.clear()


@pytest.fixture
def db_path_factory(tmp_path: Path):
    factory = DatabasePathFactory(tmp_path)
    yield factory
    factory.cleanup()


@pytest.fixture
def mem_db() -> DBConnection:
    return lancedb.connect("memory://")


@pytest.fixture
def tmp_db(db_path_factory) -> DBConnection:
    """Create a temporary database connection for testing."""
    db_path = db_path_factory("tmp_db")
    return lancedb.connect(str(db_path))


@pytest_asyncio.fixture
async def mem_db_async() -> AsyncConnection:
    return await lancedb.connect_async("memory://")
