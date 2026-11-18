# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors
# pyright: reportUnnecessaryTypeIgnoreComment=true

from datetime import timedelta
from typing import assert_type

from lancedb import AsyncTable, OptimizeOptions, Table
from lancedb._lancedb import OptimizeStats
from lancedb.job import AsyncJob, Job
from lancedb.optimize import OptimizeResult


def synchronous(table: Table) -> None:
    options: OptimizeOptions = {"target_rows_per_fragment": 100, "io_buffer_size": 0}
    assert_type(table.optimize(**options), OptimizeStats)
    assert_type(table.optimize_async(**options), Job[OptimizeResult])
    assert_type(table.optimize(cleanup_older_than=timedelta(days=1)), OptimizeStats)
    table.optimize(data_options={"max_source_rows": 1})  # pyright: ignore[reportCallIssue]
    table.optimize_async(data_options={"max_source_rows": 1})  # pyright: ignore[reportCallIssue]


async def asynchronous(table: AsyncTable) -> None:
    options: OptimizeOptions = {"target_rows_per_fragment": 100, "batch_size": 16}
    assert_type(await table.optimize(**options), OptimizeStats)
    assert_type(await table.optimize_async(**options), AsyncJob[OptimizeResult])
    await table.optimize(compaction_options={"max_source_rows": 1})  # pyright: ignore[reportCallIssue]
