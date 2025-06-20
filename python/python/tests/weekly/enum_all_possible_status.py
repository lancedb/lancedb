# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

# Weekly tests that runs all operations on a large dataset,
# these operations are ran in random order repeated 10 times

import abc
import asyncio
import itertools
import math
from typing import Optional
from lancedb.index import FTS, BTree, IvfPq
from lancedb.table import AsyncTable
import lancedb
import pyarrow as pa
import numpy as np

NUM_ROWS = 1_000_000
BATCH_SIZE = 1_000
DIM = 256

schema = pa.schema(
    [
        pa.field("id", pa.int64()),
        pa.field("vector", pa.list_(pa.float32(), DIM)),
        pa.field("text", pa.string()),
    ]
)
words = ["hello", "world", "this", "is", "a", "test", "sentence"]


def random_text(num_words: int) -> str:
    return " ".join(np.random.choice(words, num_words))


def random_batch(start_id: int, batch_size: int) -> pa.Table:
    return pa.Table.from_arrays(
        [
            pa.array(np.arange(start_id, start_id + batch_size)),
            pa.array(np.random.rand(batch_size, DIM).tolist()),
            pa.array(
                [random_text(np.random.randint(1, 10)) for _ in range(batch_size)]
            ),
        ],
        schema=schema,
    )


async def create_or_load_table(name: str, kwargs: dict):
    db = await lancedb.connect_async("tests/weekly_test_db")
    table_names = await db.table_names()
    if name in table_names:
        print("Loading existing test table")
        table = await db.open_table(name)
    else:
        print("Creating new test table")
        table = await db.create_table(name, schema=schema)
        for i in range(0, NUM_ROWS, BATCH_SIZE):
            batch = random_batch(i, BATCH_SIZE)
            await table.add(batch)
        await table.create_index("id", config=BTree(), replace=True)
        await table.create_index(
            "vector",
            config=IvfPq(
                distance_type="cosine", num_partitions=1024, num_sub_vectors=DIM // 8
            ),
            replace=True,
        )
        await table.create_index(
            "text",
            config=FTS(with_position=kwargs.get("with_position", False)),
            replace=True,
        )

    return table


class Operation(abc.ABC):
    @abc.abstractmethod
    def read_only(self) -> bool: ...

    @abc.abstractmethod
    async def run(self, table: AsyncTable): ...


class ReadOnlyOperation(Operation):
    def read_only(self) -> bool:
        return True


class WriteOperation(Operation):
    def read_only(self) -> bool:
        return False


class Append(WriteOperation):
    async def run(self, table: AsyncTable):
        batch = random_batch(await table.count_rows(), BATCH_SIZE)
        await table.add(batch)


class Delete(WriteOperation):
    async def run(self, table: AsyncTable):
        num_rows = await table.count_rows()
        to_delete = np.random.randint(0, num_rows, 100)
        to_delete = ", ".join([str(v) for v in to_delete])
        await table.delete(f"id IN ({to_delete})")


class Optimize(WriteOperation):
    async def run(self, table: AsyncTable):
        await table.optimize()


class VectorSearch(ReadOnlyOperation):
    def __init__(self, filter: Optional[str] = None):
        self.filter = filter

    async def run(self, table: AsyncTable):
        stats = await table.index_stats("vector_idx")
        if stats is None:
            print("No vector index found")
            return
        query_vector = np.random.rand(DIM).tolist()
        query = (await table.search(query_vector)).limit(10)
        if self.filter:
            query = query.where(self.filter)
        print(await query.analyze_plan())


class FullTextSearch(ReadOnlyOperation):
    def __init__(self, has_position: bool, filter: Optional[str] = None):
        self.has_position = has_position
        self.filter = filter

    async def run(self, table: AsyncTable):
        stats = await table.index_stats("text_idx")
        if stats is None:
            print("No text index found")
            return
        query_text = random_text(np.random.randint(1, 10))
        await self.do_query(table, query_text)

        if self.has_position:
            query_text = f'"{query_text}"'
            await self.do_query(table, query_text)

    async def do_query(self, table: AsyncTable, query_text: str):
        query = (await table.search(query_text)).limit(10)
        if self.filter:
            query = query.where(self.filter)
        print(await query.analyze_plan())


async def run(name: str, kwargs: dict):
    print(f"Running {name} with kwargs: {kwargs}")
    table = await create_or_load_table(name, kwargs)

    # duplicate each operation for testing idempotence
    write_operations = [
        Append(),
        Append(),
        Delete(),
        Delete(),
        Optimize(),
        Optimize(),
    ]

    has_position = kwargs.get("with_position", False)
    read_only_operations = [
        # Read only operations
        VectorSearch(),
        VectorSearch(filter="id > 1_000"),
        FullTextSearch(has_position=has_position),
        FullTextSearch(has_position=has_position, filter="id > 1_000"),
    ]

    # iterate on all permutations of write operations
    print(f"Running {math.factorial(len(write_operations))} permutations")
    for permutation in itertools.permutations(range(len(write_operations))):
        for idx in permutation:
            write_operation = write_operations[idx]
            print(f"Running {write_operation.__class__.__name__}")
            await write_operation.run(table)

            # write operation changed the status of the table,
            # then we need to run all read only operations after it
            for read_only_operation in read_only_operations:
                print(f"Running {read_only_operation.__class__.__name__}")
                await read_only_operation.run(table)


async def main():
    await run("test_table", {"with_position": False})
    await run("test_table_with_position", {"with_position": True})


if __name__ == "__main__":
    asyncio.run(main())
