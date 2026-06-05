"""End-to-end blob workflow UAT from an ML engineer's perspective."""

import os
import traceback

import numpy as np
import pyarrow as pa

import lancedb

OK, FAIL, NOTE = "PASS", "FAIL", "NOTE"
results = []


def record(scenario, status, msg):
    results.append((scenario, status, msg))
    print(f"  [{status}] {msg}")


def banner(name):
    print(f"\n=== {name} ===")


def safe(scenario, label, fn):
    try:
        fn()
    except Exception as e:
        record(scenario, FAIL, f"{label}: {type(e).__name__}: {e}")
        traceback.print_exc()


# ------------------------------------------------------------------ Scenario A
banner("A. Multimodal RAG: declare -> add -> vector search -> take_blobs")
db = lancedb.connect("memory:///")
schema_a = pa.schema(
    [
        pa.field("id", pa.int64()),
        pa.field("vector", pa.list_(pa.float32(), 8)),
        lancedb.blob("image"),
    ]
)
table_a = db.create_table("rag", schema=schema_a)
N = 5
images = {i: os.urandom(2000 + i * 17) for i in range(N)}
np.random.seed(0)
vectors = [np.random.rand(8).astype(np.float32).tolist() for _ in range(N)]
table_a.add([{"id": i, "vector": vectors[i], "image": images[i]} for i in range(N)])

hits = table_a.search(vectors[0]).limit(3).to_arrow()
record(
    "A",
    OK if "_rowid" in hits.column_names else FAIL,
    f"blob search auto-returns _rowid (cols={hits.column_names})",
)
img_type = hits["image"].type
is_struct = pa.types.is_struct(img_type)
record(
    "A",
    OK if is_struct else FAIL,
    f"blob column on query is a descriptor (struct), not bytes: {img_type}",
)

fetched = table_a.take_blobs("image", hits)
record("A", OK, f"take_blobs returned {len(fetched)} blob(s) for {len(hits)} hit(s)")

hit_id_col = hits["id"].to_pylist()
mismatched = 0
for i, hit_id in enumerate(hit_id_col):
    if fetched[i].as_py() != images[hit_id]:
        mismatched += 1
record(
    "A",
    OK if mismatched == 0 else FAIL,
    f"all {len(hit_id_col)} fetched images match input bytes (mismatched={mismatched})",
)

# ------------------------------------------------------------------ Scenario B
banner("B. Training-style: list all row ids, fetch a batch")
all_rows = table_a.search().limit(100).to_arrow()
batch_rows = all_rows.slice(0, 3)
batch = table_a.take_blobs("image", batch_rows)
record(
    "B",
    OK if len(batch) == len(batch_rows) else FAIL,
    f"batch fetch: requested {len(batch_rows)}, got {len(batch)}",
)

# ------------------------------------------------------------------ Scenario C
banner("C. Null blobs (documented misalignment)")
schema_c = pa.schema([pa.field("id", pa.int64()), lancedb.blob("image")])
table_c = db.create_table("nulls", schema=schema_c)
table_c.add([{"id": 1, "image": b"present"}, {"id": 2, "image": None}])
rows_c = table_c.search().limit(10).to_arrow()
fetched_c = table_c.take_blobs("image", rows_c)
vals_c = [fetched_c[i].as_py() for i in range(len(fetched_c))]
null_count = sum(v is None for v in vals_c)
aligned = len(fetched_c) == len(rows_c) and null_count == 1
record(
    "C",
    OK if aligned else FAIL,
    (
        f"2 rows (1 null): got {len(fetched_c)} entries, {null_count} null -> "
        f"{'aligned' if aligned else 'MISALIGNED'}"
    ),
)

# ------------------------------------------------------------------ Scenario D
banner("D. Lazy / file-like access (take_blob_files in Python)")
try:
    files = table_a.take_blob_files("image", all_rows.slice(0, 1))
    if not files:
        record("D", FAIL, f"take_blob_files returned empty: {files!r}")
    elif not hasattr(files[0], "read"):
        record("D", FAIL, f"BlobFile missing .read(): {type(files[0]).__name__}")
    else:
        data = files[0].read()
        if isinstance(data, (bytes, bytearray)) and len(data) > 0:
            record(
                "D",
                OK,
                (
                    "take_blob_files + lazy read works "
                    f"({len(data)} bytes from {len(files)} handle)"
                ),
            )
        else:
            record("D", FAIL, f"BlobFile.read returned {type(data).__name__}: {data!r}")
except AttributeError:
    record("D", NOTE, "take_blob_files NOT exposed in Python (Rust only)")
except Exception as e:
    record("D", FAIL, f"take_blob_files errored: {type(e).__name__}: {e}")

# ------------------------------------------------------------------ Scenario E
banner("E. Pydantic LanceModel with Blob (issue §1c)")
try:
    from lancedb.pydantic import LanceModel, Vector
except ImportError as e:
    record("E", NOTE, f"lancedb.pydantic not importable: {e}")
else:
    try:
        from lancedb.pydantic import Blob
    except ImportError:
        record("E", NOTE, "lancedb.pydantic.Blob NOT exposed — §1c not implemented")
    else:
        try:

            class Photo(LanceModel):
                id: int
                vector: Vector(8)
                image: Blob

            tp = db.create_table("pyd", schema=Photo)
            tp.add([{"id": 1, "vector": [0.1] * 8, "image": b"pyd-bytes"}])
            hits_p = tp.search().limit(1).to_arrow()
            got = tp.take_blobs("image", hits_p)
            record(
                "E",
                OK if got[0].as_py() == b"pyd-bytes" else FAIL,
                "Pydantic Blob round-trip works",
            )
        except Exception as e:
            record("E", FAIL, f"Pydantic path errored: {type(e).__name__}: {e}")

# ------------------------------------------------------------------ Scenario F
banner("F. Larger payload (1 MB)")
big = os.urandom(1_000_000)
table_f = db.create_table(
    "big", schema=pa.schema([pa.field("id", pa.int64()), lancedb.blob("image")])
)
table_f.add([{"id": 1, "image": big}])
hits_f = table_f.search().limit(10).to_arrow()
back = table_f.take_blobs("image", hits_f)
record(
    "F",
    OK if back[0].as_py() == big else FAIL,
    f"1 MB blob round-trips intact (got {len(back[0].as_py())} bytes)",
)

# ------------------------------------------------------------------ Scenario G
banner("G. blob_columns introspection")
cols = table_a.blob_columns()
record("G", OK if cols == ["image"] else FAIL, f"blob_columns -> {cols}")

# ------------------------------------------------------------------ Scenario H
banner("H. UX: auto _rowid on blob query results")
hits_h = table_a.search(vectors[0]).limit(3).to_arrow()
if "_rowid" in hits_h.column_names:
    out_h = table_a.take_blobs("image", hits_h)
    record("H", OK, f"plain blob query result materializes {len(out_h)} blobs")
else:
    record("H", FAIL, f"_rowid missing from blob query result: {hits_h.column_names}")

# ------------------------------------------------------------------ Scenario I
banner("I. UX: missing row-id result surfaces a helpful error")
try:
    hits_i = table_a.search(vectors[0]).limit(3).to_arrow().drop(["_rowid"])
    table_a.take_blobs("image", hits_i)
    record("I", FAIL, "expected ValueError on result missing _rowid")
except ValueError as e:
    msg = str(e)
    if "_rowid" in msg and "with_row_id" in msg:
        record("I", OK, f"clear guidance on missing _rowid: {e}")
    else:
        record("I", FAIL, f"ValueError but not the helpful message: {e}")
except Exception as e:
    record("I", FAIL, f"unexpected error type {type(e).__name__}: {e}")

# ------------------------------------------------------------------ Scenario J
banner("J. take_blobs(descriptions) one-arg form (issue §3b)")
# The bare-column form is design-blocked at the Lance layer (descriptor doesn't
# carry the row address). Verify the user gets a clear redirect, and that the
# convenient `take_blobs(column, query_result_table)` form works end-to-end.
descriptions = hits["image"]
try:
    table_a.take_blobs("image", descriptions)
    record("J", FAIL, "expected ValueError on bare descriptor column")
except ValueError as e:
    if "row address" in str(e) or "query result table" in str(e):
        record("J", OK, f"clear redirect for bare descriptor: {str(e)[:80]}...")
    else:
        record("J", FAIL, f"ValueError but not the helpful one: {e}")
out = table_a.take_blobs("image", hits)
if len(out) == len(hits):
    record(
        "J",
        OK,
        f"take_blobs(column, query_result_table) honored §3b intent: {len(out)} blobs",
    )
else:
    record("J", FAIL, f"length mismatch: {len(out)} vs {len(hits)}")

# ------------------------------------------------------------------ Scenario K
banner("K. take_blobs error paths")
try:
    table_a.take_blobs("id", [0])
    record("K", FAIL, "expected ValueError on non-blob column, got success")
except ValueError as e:
    record("K", OK, f"non-blob column -> ValueError: {e}")
except Exception as e:
    record("K", FAIL, f"non-blob column -> wrong type {type(e).__name__}: {e}")

try:
    table_a.take_blobs("nope", [0])
    record("K", FAIL, "expected ValueError on missing column, got success")
except ValueError as e:
    record("K", OK, f"missing column -> ValueError: {e}")
except Exception as e:
    record("K", FAIL, f"missing column -> wrong type {type(e).__name__}: {e}")

# ------------------------------------------------------------------ Scenario L
banner("L. Bytes-only column passed as plain LargeBinary (not blob)")
schema_plain = pa.schema(
    [pa.field("id", pa.int64()), pa.field("data", pa.large_binary())]
)
table_l = db.create_table("plain", schema=schema_plain)
table_l.add([{"id": 1, "data": b"hi"}])
record(
    "L",
    OK if table_l.blob_columns() == [] else FAIL,
    (
        "plain LargeBinary column NOT reported as blob: "
        f"blob_columns()={table_l.blob_columns()}"
    ),
)

# ------------------------------------------------------------------ Scenario M
banner("M. Bulk ingest (50 rows)")
schema_m = pa.schema(
    [
        pa.field("id", pa.int64()),
        pa.field("vector", pa.list_(pa.float32(), 8)),
        lancedb.blob("image"),
    ]
)
table_m = db.create_table("bulk", schema=schema_m)
M = 50
data_m = [
    {
        "id": i,
        "vector": np.random.rand(8).astype(np.float32).tolist(),
        "image": os.urandom(500 + i),
    }
    for i in range(M)
]
table_m.add(data_m)
all_m = table_m.search().limit(1000).to_arrow()
record(
    "M",
    OK if all_m.num_rows == M else FAIL,
    f"bulk ingest {M} rows; query saw {all_m.num_rows}",
)

# ------------------------------------------------------------------ Summary
print("\n=== summary ===")
counts = {}
for _, status, _ in results:
    counts[status] = counts.get(status, 0) + 1
print(" ".join(f"{k}={v}" for k, v in counts.items()))
for scenario, status, msg in results:
    print(f"  {scenario} [{status}] {msg}")
