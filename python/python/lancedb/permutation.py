# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

from ._lancedb import async_permutation_builder
from .table import LanceTable
from .db import LanceDBConnection
from .background_loop import LOOP
from typing import Optional


class PermutationBuilder:
    def __init__(
        self, table: LanceTable, conn: LanceDBConnection, dest_table_name: str
    ):
        self._conn = conn
        self._async = async_permutation_builder(table, conn, dest_table_name)

    def select(self, projections: dict[str, str]) -> "PermutationBuilder":
        self._async.select(projections)
        return self

    def split_random(
        self,
        *,
        ratios: Optional[list[float]] = None,
        counts: Optional[list[int]] = None,
        fixed: Optional[int] = None,
        seed: Optional[int] = None,
    ) -> "PermutationBuilder":
        self._async.split_random(ratios=ratios, counts=counts, fixed=fixed, seed=seed)
        return self

    def split_hash(
        self,
        columns: list[str],
        split_weights: list[int],
        *,
        discard_weight: Optional[int] = None,
    ) -> "PermutationBuilder":
        self._async.split_hash(columns, split_weights, discard_weight=discard_weight)
        return self

    def split_sequential(
        self,
        *,
        ratios: Optional[list[float]] = None,
        counts: Optional[list[int]] = None,
        fixed: Optional[int] = None,
    ) -> "PermutationBuilder":
        self._async.split_sequential(ratios=ratios, counts=counts, fixed=fixed)
        return self

    def split_calculated(self, calculation: str) -> "PermutationBuilder":
        self._async.split_calculated(calculation)
        return self

    def shuffle(
        self, *, seed: Optional[int] = None, clump_size: Optional[int] = None
    ) -> "PermutationBuilder":
        self._async.shuffle(seed=seed, clump_size=clump_size)
        return self

    def filter(self, filter: str) -> "PermutationBuilder":
        self._async.filter(filter)
        return self

    def execute(self) -> LanceTable:
        async def do_execute():
            inner_tbl = await self._async.execute()
            return LanceTable.from_inner(self._conn, inner_tbl)

        return LOOP.run(do_execute())


def permutation_builder(
    table: LanceTable, conn: LanceDBConnection, dest_table_name: str
) -> PermutationBuilder:
    return PermutationBuilder(table, conn, dest_table_name)
