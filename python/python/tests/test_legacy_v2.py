from unittest.mock import AsyncMock, Mock

import pytest

from lancedb import legacy_v2


class FakeQuery:
    def __init__(self, error=None):
        self.error = error

    def limit(self, _limit):
        return self

    async def to_arrow(self):
        if self.error is not None:
            raise self.error
        return None


class FakeAsyncTable:
    def __init__(self, rows=2, error=None):
        self.rows = rows
        self.error = error

    def query(self):
        return FakeQuery(self.error)

    async def count_rows(self):
        return self.rows


class FakeDataset:
    def __init__(self, reader, rows=2):
        self.reader = reader
        self.rows = rows

    def count_rows(self):
        return self.rows

    def scanner(self):
        scanner = Mock()
        scanner.to_reader.return_value = self.reader
        return scanner


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "message",
    [
        "rust future panicked: unknown error",
        "Panic in async function",
        "Missing ColumnEncoding encoding description",
        "Missing lance.encodings.ColumnEncoding encoding description",
        "the column at index 0 was missing a ColumnEncoding",
    ],
)
async def test_needs_migration_recognizes_legacy_reader_errors(message):
    db = Mock()
    db.open_table = AsyncMock(return_value=FakeAsyncTable(error=RuntimeError(message)))

    assert await legacy_v2._needs_migration(db, "legacy")


@pytest.mark.asyncio
async def test_needs_migration_propagates_unrelated_errors():
    db = Mock()
    db.open_table = AsyncMock(
        return_value=FakeAsyncTable(error=RuntimeError("permission denied"))
    )

    with pytest.raises(RuntimeError, match="permission denied"):
        await legacy_v2._needs_migration(db, "legacy")


@pytest.mark.asyncio
async def test_migration_streams_and_verifies_rows(monkeypatch):
    reader = object()
    source_dataset = FakeDataset(reader)
    source_table = Mock()
    source_table.to_lance.return_value = source_dataset
    source_db = Mock()
    source_db.list_tables.return_value.tables = ["healthy", "legacy"]
    source_db.open_table.return_value = source_table

    legacy_error = RuntimeError(
        "Missing lance.encodings.ColumnEncoding encoding description"
    )
    async_source_db = Mock()

    async def open_table(name):
        if name == "legacy":
            return FakeAsyncTable(error=legacy_error)
        return FakeAsyncTable()

    async_source_db.open_table = open_table
    create_calls = []

    async def create_table(name, data, *, mode, storage_options):
        create_calls.append((name, data, mode, storage_options))
        return FakeAsyncTable()

    async_source_db.create_table = create_table

    monkeypatch.setattr(legacy_v2, "_require_legacy_lance", Mock())
    monkeypatch.setattr(legacy_v2.lancedb, "connect", Mock(return_value=source_db))

    async def connect_async(_uri):
        return async_source_db

    monkeypatch.setattr(legacy_v2.lancedb, "connect_async", connect_async)

    migrated = await legacy_v2.migrate_legacy_v2_tables("/data/db", show_progress=False)

    assert migrated == ["legacy"]
    source_db.list_tables.assert_called_once_with(limit=None)
    assert create_calls == [
        (
            "legacy",
            reader,
            "overwrite",
            {"new_table_data_storage_version": "2.0"},
        )
    ]


def test_list_table_names_supports_legacy_connection():
    db = Mock(spec=["table_names"])
    db.table_names.return_value = [f"table_{index}" for index in range(12)]

    assert len(legacy_v2._list_table_names(db)) == 12


@pytest.mark.asyncio
async def test_create_table_uses_legacy_storage_parameter():
    calls = []

    class LegacyConnection:
        async def create_table(self, name, data, *, mode, data_storage_version=None):
            calls.append((name, data, mode, data_storage_version))
            return FakeAsyncTable()

    reader = object()
    await legacy_v2._create_migrated_table(LegacyConnection(), "legacy", reader, "0.1")

    assert calls == [("legacy", reader, "overwrite", "0.1")]


def test_requires_exact_legacy_lance_version(monkeypatch):
    fake_lance = Mock(__version__="9.0.0")
    monkeypatch.setitem(__import__("sys").modules, "lance", fake_lance)

    with pytest.raises(RuntimeError, match="requires pylance==0.12.1"):
        legacy_v2._require_legacy_lance()


@pytest.mark.asyncio
async def test_cloud_migration_is_rejected_before_dependency_check(monkeypatch):
    require_lance = Mock()
    monkeypatch.setattr(legacy_v2, "_require_legacy_lance", require_lance)

    with pytest.raises(ValueError, match="only for local/OSS"):
        await legacy_v2.migrate_legacy_v2_tables("db://example")

    require_lance.assert_not_called()
