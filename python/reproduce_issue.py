#!/usr/bin/env python3
"""
Reproduction script for issue #2465: FTS explain plan is incorrect
"""

import pyarrow as pa
import lancedb

# Create test data
data = pa.table({
    "id": range(4),
    "text": [
        "This is a test",
        "This is another test",
        "This is a third test",
        "This is a fourth test"
    ],
    "vector": pa.array([[1.0, 2.0], [3.0, 4.0], [5.0, 6.0], [7.0, 8.0]],
                       type=pa.list_(pa.float32(), list_size=2))
})

# Create database and table
db = lancedb.connect("test")
table = db.create_table("test_table", data, mode="overwrite")
table.create_fts_index("text")

# Test the explain plan for FTS query with limit and offset
print("=== FTS Query with limit and offset ===")
plan = (
    table.search("test", query_type="fts", fts_columns="text")
    .offset(2)
    .limit(4)
    .explain_plan()
)
print(plan)
print()

# Test pure FTS query without limit/offset
print("=== Pure FTS Query ===")
plan2 = table.search("test", query_type="fts", fts_columns="text").explain_plan()
print(plan2)
print()

# Test vector query for comparison
print("=== Vector Query for comparison ===")
plan3 = table.search([1.0, 2.0]).explain_plan()
print(plan3)
