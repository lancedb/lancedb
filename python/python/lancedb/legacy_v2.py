# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Recovery utilities for the experimental V2 format used by old Node releases."""

from __future__ import annotations

import argparse
import asyncio
import inspect
import warnings
from collections.abc import Iterable
from typing import Any

from packaging.version import Version

import lancedb

__all__ = ["migrate_legacy_v2_tables"]

_LEGACY_LANCE_VERSION = Version("0.12.1")
_LEGACY_V2_ERROR_MARKERS = (
    "missing columnencoding encoding description",
    "missing lance.encodings.columnencoding encoding description",
    "was missing a columnencoding",
    "rust future panicked",
    "panic in async function",
)


def _require_legacy_lance() -> Any:
    try:
        import lance
    except ImportError as error:
        raise RuntimeError(
            "Legacy V2 migration requires pylance==0.12.1. Install it in a "
            "dedicated environment with "
            "`pip install --no-deps pylance==0.12.1`."
        ) from error

    version = Version(lance.__version__)
    if version != _LEGACY_LANCE_VERSION:
        raise RuntimeError(
            "Legacy V2 migration requires pylance==0.12.1, but found "
            f"pylance=={version}. Reinstall it with "
            "`pip install --no-deps --force-reinstall pylance==0.12.1`."
        )
    return lance


def _exception_messages(error: BaseException) -> Iterable[str]:
    seen: set[int] = set()
    current: BaseException | None = error
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        yield str(current).lower()
        current = current.__cause__ or current.__context__


def _is_legacy_v2_error(error: BaseException) -> bool:
    return any(
        marker in message
        for message in _exception_messages(error)
        for marker in _LEGACY_V2_ERROR_MARKERS
    )


def _list_table_names(db: Any) -> list[str]:
    list_tables = getattr(db, "list_tables", None)
    if list_tables is not None:
        # The deprecated table_names() API defaults to only ten results.
        return list(list_tables(limit=None).tables)

    # Compatibility for LanceDB 0.16, which was used by the original script.
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        return list(db.table_names())


async def _needs_migration(db: Any, table_name: str) -> bool:
    table = await db.open_table(table_name)
    try:
        # One row is enough to load and validate the data-file metadata.
        await table.query().limit(1).to_arrow()
    except (KeyboardInterrupt, SystemExit, GeneratorExit):
        raise
    except BaseException as error:
        if _is_legacy_v2_error(error):
            return True
        raise
    return False


async def _create_migrated_table(
    db: Any, table_name: str, reader: Any, storage_format: str
) -> Any:
    parameters = inspect.signature(db.create_table).parameters
    options: dict[str, Any] = {"mode": "overwrite"}
    if "data_storage_version" in parameters:
        # LanceDB 0.16 exposed the format as a direct create_table option.
        options["data_storage_version"] = storage_format
    else:
        options["storage_options"] = {"new_table_data_storage_version": storage_format}
    return await db.create_table(table_name, reader, **options)


async def _migrate_table(
    source_db: Any,
    destination_db: Any,
    table_name: str,
    storage_format: str,
) -> int:
    source_table = source_db.open_table(table_name)
    source_dataset = source_table.to_lance()
    source_rows = source_dataset.count_rows()
    reader = source_dataset.scanner().to_reader()

    migrated_table = await _create_migrated_table(
        destination_db, table_name, reader, storage_format
    )
    migrated_rows = await migrated_table.count_rows()
    if migrated_rows != source_rows:
        raise RuntimeError(
            f"Migration of table {table_name!r} wrote {migrated_rows} rows; "
            f"expected {source_rows}."
        )

    # Force the current reader to load data-file metadata before reporting success.
    await migrated_table.query().limit(1).to_arrow()
    return migrated_rows


async def migrate_legacy_v2_tables(
    uri: str,
    *,
    table_name: str | None = None,
    destination_uri: str | None = None,
    storage_format: str = "2.0",
    show_progress: bool = True,
) -> list[str]:
    """Migrate tables written with the incompatible experimental V2 format.

    LanceDB Node 0.5.x could enable an experimental data format when an empty
    table was created and data was added later. Those files panic older modern
    readers and are rejected by newer readers. This utility streams them through
    ``pylance==0.12.1`` and rewrites them in a supported format.

    Install the legacy reader in a dedicated environment before running this
    function::

        pip install lancedb
        pip install --no-deps pylance==0.12.1

    ``--no-deps`` is required because the legacy wheel declares an obsolete
    PyArrow upper bound. The migration uses only its dataset scanner and writes
    through the current LanceDB package.

    This migration is available only for local/OSS databases, including object
    storage URIs. It is not supported for LanceDB Cloud ``db://`` connections.
    In-place migration creates a new table version, so old data remains available
    for recovery until old versions are cleaned up. Table indices are not copied
    and should be rebuilt after migration.

    Parameters
    ----------
    uri : str
        Source LanceDB database URI.
    table_name : str, optional
        Migrate only this table. By default, inspect every table.
    destination_uri : str, optional
        Write to another database. By default, migrate in place.
    storage_format : str, default "2.0"
        Data storage format for the rewritten tables. Use ``"0.1"`` for
        compatibility with older LanceDB releases.
    show_progress : bool, default True
        Display progress bars while inspecting and migrating tables.

    Returns
    -------
    list of str
        Names of the migrated tables.
    """
    if uri.startswith("db://") or (
        destination_uri is not None and destination_uri.startswith("db://")
    ):
        raise ValueError("Legacy V2 migration is supported only for local/OSS tables")

    # Import and validate before opening or modifying any table.
    _require_legacy_lance()

    source_db = lancedb.connect(uri)
    async_source_db = await lancedb.connect_async(uri)
    destination_db = (
        async_source_db
        if destination_uri is None or destination_uri == uri
        else await lancedb.connect_async(destination_uri)
    )

    if table_name is not None:
        table_names = [table_name]
    else:
        table_names = _list_table_names(source_db)

    inspection: Iterable[str] = table_names
    if show_progress:
        from tqdm.auto import tqdm

        inspection = tqdm(table_names, desc="Checking tables")

    tables_to_migrate = [
        name for name in inspection if await _needs_migration(async_source_db, name)
    ]

    migration: Iterable[str] = tables_to_migrate
    if show_progress:
        from tqdm.auto import tqdm

        migration = tqdm(tables_to_migrate, desc="Migrating tables")

    migrated = []
    for name in migration:
        await _migrate_table(source_db, destination_db, name, storage_format)
        migrated.append(name)
    return migrated


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Migrate tables written with the old experimental V2 format."
    )
    parser.add_argument("uri", help="source LanceDB database URI")
    parser.add_argument("--table-name", help="migrate only this table")
    parser.add_argument("--destination-uri", help="write to another database URI")
    parser.add_argument(
        "--storage-format",
        default="2.0",
        help='destination data format (default: "2.0"; use "0.1" for compatibility)',
    )
    parser.add_argument(
        "--no-progress", action="store_true", help="disable progress bars"
    )
    return parser


def main() -> None:
    args = _parser().parse_args()
    migrated = asyncio.run(
        migrate_legacy_v2_tables(
            args.uri,
            table_name=args.table_name,
            destination_uri=args.destination_uri,
            storage_format=args.storage_format,
            show_progress=not args.no_progress,
        )
    )
    if migrated:
        print(f"Migrated {len(migrated)} table(s): {', '.join(migrated)}")
    else:
        print("No legacy V2 tables found")


if __name__ == "__main__":
    main()
