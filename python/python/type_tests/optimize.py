# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors
# pyright: reportUnnecessaryTypeIgnoreComment=true

from datetime import timedelta
from typing import assert_type

from lancedb import AsyncTable, OptimizeOptions, Table
from lancedb._lancedb import OptimizeStats
from lancedb.job import AsyncJob, Job
from lancedb.optimize import OptimizePlan, OptimizeResult


def synchronous(table: Table) -> None:
    options: OptimizeOptions = {"target_rows_per_fragment": 100, "io_buffer_size": 0}
    assert_type(table.optimize(**options), OptimizeStats)
    assert_type(table.optimize(retrain=False), OptimizeStats)
    table.optimize_async(retrain=True)  # pyright: ignore[reportCallIssue]
    assert_type(table.optimize_async(**options), Job[OptimizeResult])
    assert_type(table.explain_optimize(**options), OptimizePlan)
    table.explain_optimize(data_options={})  # pyright: ignore[reportCallIssue]
    assert_type(table.optimize(cleanup_older_than=timedelta(days=1)), OptimizeStats)
    table.optimize(data_options={"max_source_rows": 1})  # pyright: ignore[reportCallIssue]
    table.optimize_async(data_options={"max_source_rows": 1})  # pyright: ignore[reportCallIssue]


async def asynchronous(table: AsyncTable) -> None:
    options: OptimizeOptions = {"target_rows_per_fragment": 100, "batch_size": 16}
    assert_type(await table.optimize(**options), OptimizeStats)
    assert_type(await table.optimize_async(**options), AsyncJob[OptimizeResult])
    await table.optimize(compaction_options={"max_source_rows": 1})
    assert_type(await table.explain_optimize(**options), OptimizePlan)


async def legacy_types(table: AsyncTable) -> None:
    from lancedb import CompactionOptions
    from lancedb.compaction import CompactionResult

    options: CompactionOptions = {"max_source_rows": 7}
    assert_type(await table.optimize(compaction_options=options), OptimizeStats)
    assert_type(
        await table.compact_async(compaction_options=options),
        AsyncJob[CompactionResult],
    )


def preview_types(table: Table, options: OptimizeOptions) -> None:
    assert_type(table.explain_optimize(**options), OptimizePlan)
    table.explain_optimize(compaction_options={})
