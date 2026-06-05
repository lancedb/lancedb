# Blob Column Support in LanceDB

Author: Drew Gallardo

**Status:** Design proposal (extends [#3231](https://github.com/lancedb/lancedb/issues/3231))

This doc spells out the API we want for first-class blob columns in LanceDB: declare, write, query, and materialize large binary payloads without dropping to lance-core.

The thing I want to keep clear: Lance core supports a few ways to spell a Blob V2 schema, but LanceDB should expose one recommended path first. The other valid paths stay available as lower-level escape hatches; we don't lead with them. Same idea for write, read, and materialize.

## Gap today

Lance already has Blob V2 at the storage layer. LanceDB does not expose it as a public API. Today you have two options:

- Store large payloads in a normal binary column. This works, but a query that returns the column materializes the full bytes for every row, which costs you on every scan.
- Drop to `table.to_lance().take_blobs(...)` from lance-core. It works in Python and Rust today; Node has no `to_lance()` equivalent.

The gap is product surface. We have the storage. We need a clean way to declare blob columns, write data, and read with the right mode (descriptors, lazy handles, materialized bytes), without pushing users into lance-core internals.

## Proposed UX

Every operation defaults to cheap. Bytes move when you ask for them.

End-to-end flow:

```python
import lancedb
from lancedb.pydantic import LanceModel, Vector, Blob

class Photo(LanceModel):
    id: int
    vector: Vector(768)
    image: Blob

db = lancedb.connect("./photos.db")
table = db.create_table("photos", schema=Photo)

table.add([
    {"id": 1, "vector": [0.1] * 768, "image": open("cat.jpg", "rb").read()},
    {"id": 2, "vector": [0.2] * 768, "image": open("dog.jpg", "rb").read()},
])

query_vec = [0.15] * 768  # an embedding from your model
hits = table.search(query_vec).limit(10).to_arrow()
# hits["image"] is a small descriptor struct, not bytes

images = table.take_blobs("image", hits)
```

That covers the whole loop: declare, insert, search, materialize. The example shows the proposed UX; how LanceDB carries the row address through the query so the user doesn't have to is Open Decision 1.

## Schema declaration (`create_table`)

Blobs live in the schema you hand to `create_table`. We can't infer this from raw binary columns, so the declaration is explicit and up front. `create_table` reads the schema, picks up the blob marker, and bumps the table to format ≥ 2.2.

Under the hood every path below produces the canonical Blob V2 Arrow field the pinned Lance engine recognizes: a `Struct<data, uri>` with the `lance.blob.v2` extension marker. The choice is about how you'd rather spell it on the way in.

### Recommended: PyArrow helper

For pyarrow schemas, lead with `lancedb.blob("image")`. Drops into a `pa.schema()` like any other field helper.

```python
import lancedb
import pyarrow as pa

schema = pa.schema([
    pa.field("id", pa.int64()),
    lancedb.blob("image"),
])
```

### Recommended: Pydantic `Blob`

For LanceModel schemas, lead with `Blob`. Pydantic counterpart in the role `Vector(768)` plays for vectors.

```python
from lancedb.pydantic import LanceModel, Blob

class Photo(LanceModel):
    id: int
    image: Blob
```

### Also accepted: hand-written V2 field

You can construct the V2 field directly. This is exactly what the helper builds. Useful if you want to see the seam, or if you'd rather not pull a LanceDB import into a pyarrow schema file.

```python
schema = pa.schema([
    pa.field("id", pa.int64()),
    pa.field(
        "image",
        pa.struct([
            pa.field("data", pa.large_binary()),
            pa.field("uri", pa.string()),
        ]),
        metadata={"ARROW:extension:name": "lance.blob.v2"},
    ),
])
```

This stays supported, but it isn't the main UX.

Already have a format-2.1 table with the V1 marker (`large_binary` + `lance-encoding:blob`)? It isn't a path for new tables (Lance rejects it at format ≥ 2.2), but it keeps working through `to_lance()`. See the V1 legacy row in Limitations.

## Insert (`table.add`)

Bytes go in like any other column. LanceDB coerces them into the V2 struct so users never build it themselves.

### Recommended: bytes in a dict

Pass `bytes` in a dict, list of dicts, or an Arrow batch. LanceDB coerces `LargeBinary` or `Binary` input into the `Struct<data, uri>` array before the write.

```python
table.add([
    {"id": 1, "image": open("cat.jpg", "rb").read()},
    {"id": 2, "image": open("dog.jpg", "rb").read()},
])
```

### Also accepted: pre-built struct

If you already have a pyarrow Table with the V2 struct built correctly, hand it over. Symmetric to writing the V2 field by hand in Schema declaration: you provide the struct, we skip coercion.

```python
import pyarrow as pa

tbl = pa.Table.from_arrays(
    [
        pa.array([1, 2]),
        pa.StructArray.from_arrays(
            [
                pa.array([b"cat", b"dog"], type=pa.large_binary()),
                pa.array(["", ""], type=pa.string()),
            ],
            names=["data", "uri"],
        ),
    ],
    names=["id", "image"],
)
table.add(tbl)
```

### Deferred: storage URIs

External reference by default; `move_blobs=True` makes Lance own the bytes.

```python
table.add([
    {"id": 1, "image": "s3://bucket/cat.jpg"},
    {"id": 2, "image": "/local/path/dog.jpg"},
])
```

## Read (query)

Query results should be cheap by default. If a table has a blob column, returning that column from a query should not materialize the bytes. It should return a small descriptor.

### Default: descriptors

Blob columns come back as small structs, not bytes. Search stays cheap. The usual terminals all work: `.to_arrow()`, `.to_list()`, `.to_pandas()`. Matches the shape in issue §3a.

```python
hits = table.search(query_vec).limit(10).to_arrow()
# hits["image"] is a struct: {kind, position, size, blob_id, blob_uri}
```

When a query result includes a blob column, LanceDB automatically includes `_rowid` so the result table can be passed to `take_blobs` without a separate row-id opt-in.

You materialize bytes with `take_blobs` (next section). Inline byte materialization directly in query results is deferred out of Phase 1.

## Materialize bytes

The user-facing API hides row IDs for the common path. Lance's resolver still needs `(descriptor, row_address)` internally, so Python automatically asks for `_rowid` when a query projection returns a blob column.

`take_blobs` is a dedicated blob-materialization call on the table. It is not a flavor of `take_row_ids` (which is a builder for loading arbitrary columns).

### Recommended: pass the query result

Pass the query result table and the column name.

```python
images = table.take_blobs("image", hits)
```

The result is aligned 1:1 with the rows in `hits`. Rows whose blob is null come back as null in the array.

### Lazy handles

For large payloads or selective reads; bytes load on `.read()`.

```python
handles = table.take_blob_files("image", hits)
data = handles[0].read()
```

### Lower-level: row IDs

Rust's `Dataset::take_blobs` takes `&[u64]` of row ids. Python wraps that with the result-passing form above so users don't handle row IDs. If you do need the row-id form (you're working at the Rust layer, or you already have row ids from somewhere else), it's there:

```python
images = table.take_blobs("image", list_of_row_ids)
```

`take_blobs` delegates to Lance `Dataset::take_blobs(row_ids, column)`. LanceDB extracts the row IDs, forwards the call, and stitches nulls back so the output stays aligned with what you asked for.

If you don't even want descriptors in the result, drop the blob column from the projection: `table.search(vec).select(["id", "vector"]).to_arrow()`.

## Why this shape

- The default is cheap. You declare, you query, and nothing big has moved. Bytes move when you ask.
- It wraps Lance Blob V2 instead of inventing a new format. No separate blob store to operate.
- It meets people where they define schemas: pyarrow helper, Pydantic type, power-user metadata, legacy `to_lance()` unchanged.

| Option | Where it lands |
|---|---|
| Binary columns (today) | Keep. Fine for small payloads |
| Blob column + `take_blobs` | What we're proposing |
| URLs or paths in a string column | Keep. Apps can still use S3 or disk directly |
| Low-level Lance only (`to_lance()`) | Stays for advanced users |
| Full bytes in every query by default | No. Recreates the bandwidth problem |

## How this fits LanceDB

Fits existing patterns:

- Schema helper on `LanceModel`: Pydantic `Blob` mirrors `Vector(768)`.
- Coerce-on-add: same shape we use elsewhere when the input batch and table schema disagree.
- Default-cheap query, opt-in materialize: same shape as workflows that query first and load more on the hits, without surfacing row IDs in user code.
- Cross-SDK names match: Rust `take_blobs`, Python `take_blobs`, future TS `takeBlobs`.

New because Lance requires it:

- The blob field is `Struct<data, uri>` with the `lance.blob.v2` extension marker. Flat `LargeBinary` is not a path at format ≥ 2.2.
- Format auto-bump to 2.2 on blob declaration, parallel to the existing storage-version override hooks.
- A dedicated materialize API. `take_blobs` is not a variant of `take_row_ids`; Lance pairs descriptors with row addresses internally, so the take path is a separate API rather than a column projection through the query builder.
- The pyarrow helper `lancedb.blob()` returns a field today, not a type. `lancedb.vector(768)` returns a type, so the spellings differ in raw pyarrow until we own a version-locked extension type (Open Decision 2).

## API conventions (the vector-DB bar)

Every major vector DB makes heavy stored data opt-in through a field selector, never inline by default. The design here follows that shape: descriptors by default, then an explicit materialization call. Descriptors plus deferred materialize is what separates blob v2 from Weaviate's base64 payload.

> Status: Phase 1 ships declaration, byte writes, descriptors by default, auto `_rowid` for blob query results, `take_blobs`, and `take_blob_files` for local tables. Inline byte query results are out of scope for this PR.

| System | Stored fields on read | Opt-in selector | Large objects |
|---|---|---|---|
| Pinecone | id + score | `include_metadata` / `include_values` (bool) | none; 40 KB metadata cap, store an S3 reference |
| Qdrant | id + score | `with_payload` / `with_vectors` (`bool \| list \| {exclude}`) | discouraged; use an external store |
| Weaviate | properties (gRPC) | `return_metadata` | first-class `blob`, but inline base64 only, no lazy read |
| Milvus | primary key only | `output_fields=[...]` / `["*"]` | none; external store + reference |
| Chroma | id only | `.select(...)` / `.select_all()` | `uris` field (external reference) |
| Turbopuffer | id only | `include_attributes` (`list \| true`) | none; external store + reference |
| LanceDB blobs | descriptors | `take_blobs(...)` | first-class, descriptor + lazy materialize |

What we keep, because it matches the bar:

- Heavy data is opt-in. Descriptors by default is the universal principle, and the right call for multi-GB columns.
- Materialization is explicit and table-scoped. `take_blobs("image", hits)` makes the expensive read visible without changing query result shape.
- Bad column names fail loud. `take_blobs("not_a_blob", ...)` raises with the column name instead of the silent omission Chroma and Turbopuffer fall back to. A blob read that quietly returns nothing is a 2am debugging trap.

What to tighten:

- **Lead with the differentiator.** Weaviate is the only other first-class blob type and it must serve the whole payload base64-encoded on every read. Descriptor + deferred materialize is strictly better; the docs should sell it as the reason to use a blob column over a plain binary field.
- **The `take_blobs` verb leaks Arrow internals.** No surveyed vector DB exposes `take` for retrieval; Pinecone uses `fetch`, Milvus `get`. See Open Decision 6.

## Table type support

Blobs are local-first. Descriptors ride inline in the existing query response on every backend; byte materialization needs new remote plumbing.

| Capability | Local | Remote (Cloud) | Namespace |
|---|---|---|---|
| Declare blob column, write bytes | yes | yes | yes (`apply_blob_storage_version` runs in `database/namespace.rs`) |
| `blob_columns()` | yes | `NotSupported` in Python until cloud support lands | yes via schema |
| Descriptors on read | yes | yes (rides in the Arrow IPC body) | yes |
| Inline bytes in query result | out of Phase 1 | `NotSupported` | `NotSupported` |
| `take_blobs` / `take_blob_files` | yes | `NotSupported` (no endpoint yet) | `NotSupported` (no data op yet) |

Parity follows the `uses_v2_manifest_paths` precedent: the `BaseTable` trait method defaults to `NotSupported`, `NativeTable` overrides it, and `RemoteTable` stays gated until a `server_version.support_blobs()` flag flips when the endpoint ships.

### What the remote protocol carries today

The query endpoint is `POST /v1/table/{id}/query/`. Body shape (see `apply_query_params` in `remote/table.rs`):

```json
{
  "version": "...",
  "k": 10,
  "prefilter": true,
  "vector": [...],
  "filter": "...",
  "columns": ["..."],
  "with_row_id": true,
  "fast_search": false,
  "offset": 0
}
```

Response: a fully buffered Arrow IPC FileReader (`read_arrow_stream` in `remote/table.rs`). The body has no inline blob-materialization field. Streaming responses are blocked on [arrow-rs#6420](https://github.com/apache/arrow-rs/issues/6420). The freshness header rides every read; retries go through `send_with_retry`.

### What blob support adds to the protocol

- `POST /v1/table/{id}/blobs/columns/` returns the names of blob v2 fields. Computable from the schema endpoint server-side; the wrapper is thin.
- `POST /v1/table/{id}/take_blobs/` takes `{ "version", "column", "row_ids" }` and returns a null-aligned `LargeBinaryArray` as Arrow IPC. Reuses the auth, retry, and freshness path of `/query/`. The buffered response is the limit: payloads above ~hundreds of MB belong on the pre-signed path below.
- `POST /v1/table/{id}/take_blobs/sign/` (Phase 4) takes the same body and returns a list of pre-signed URLs, one per row id, null where the blob is null. The client fetches each URL out of band. Lance v2 External storage mode already encodes `(uri, offset, size)` per blob; the cloud's signing service issues a per-blob URL from that triple. Deferred per [#3231](https://github.com/lancedb/lancedb/issues/3231) Phase 4 and needs the signing service the issue flags as open.

The cloud already moves bytes outside the Arrow body on the write side: `/v1/table/{id}/multipart_write/{create,complete,abort}`. The signed-URL read path is the inversion of that pattern.

### Namespace

`apply_blob_storage_version` runs on the namespace write path (see `database/namespace.rs:213`), so `LanceNamespace` can create and write blob tables today. Two read-side gaps:

- `QueryTableRequest` has no inline blob-materialization field; descriptors-by-default works through the normal query.
- A new `TakeBlobs` data operation has to land in `lance-namespace` before `take_blobs` can dispatch through it.

## Limitations

| Topic | Plain answer |
|---|---|
| Cloud | Out of scope; local tables first |
| TypeScript / Node | Parity required per [#3231](https://github.com/lancedb/lancedb/issues/3231); same API surface as Python and Rust |
| Two-step read | Query first, materialize bytes second |
| New tables | Declare the blob column at `create_table` |
| File format | Blob tables use Lance format 2.2; other new tables stay on 2.1 |
| V1 legacy | Existing tables with the V1 marker (`large_binary` + `lance-encoding:blob`) on format 2.1 keep working through `to_lance()`. Lance rejects the V1 marker at format ≥ 2.2; new blob columns must be V2 |
| Compaction | `optimize(Compact)` preserves blob reads with the current pinned Lance version; `compacting_blob_table_preserves_blob_round_trip` covers this. |
| Null blobs | Output aligned 1:1 with what you asked for; null blob rows stay null |
| Row ids | Row addresses can change after compaction or rewrite unless stable row ids are enabled (open decision) |

## Decision summary

| Area | What we'd lead with | Also accepted / later |
|---|---|---|
| Schema | `lancedb.blob("image")` or Pydantic `Blob` | Hand-written V2 Arrow field |
| Write | Bytes in `table.add(...)` | Pre-built struct; URIs later |
| Query | Descriptors by default | Inline bytes in query results deferred |
| Materialize bytes | `take_blobs("image", hits)` | Row-id form for Rust or power users |
| Lazy read | `take_blob_files("image", hits)` | Future SDK helpers |
| Remote tables | `NotSupported` for now | REST support later |

## Open decisions

### 1. Row IDs for `take_blobs`

Issue §3b is `table.take_blobs(descriptions)`. Users should not have to call `.with_row_id(True)` just to materialize blob bytes. Lance's resolver needs `(descriptor, row_address)` and our V2 descriptor doesn't carry the address, so Phase 1 chooses the smallest wrapper-level plumbing:

- If a Python query result includes a blob column, the query includes `_rowid` automatically.
- `take_blobs("image", hits)` and `take_blob_files("image", hits)` read `_rowid` from the result table.
- `take_blobs("image", list_of_row_ids)` stays available for Rust and power users.

This does not change the Lance descriptor shape and does not add a new Lance format feature.

### 2. `lancedb.blob()` shape

Field factory (`lancedb.blob("image")`) vs registered extension type (`pa.field("image", lancedb.blob())`) like `vector(dim)`. Field metadata is the safe default when the engine and `pylance` can disagree on struct layout; owning a version-locked extension type is the path to vector-style ergonomics.

### 3. Stable row ids on create

Auto-bump to 2.2 sets a precedent. Should blob tables also enable stable row ids so bytes still materialize correctly after a compaction or rewrite ([#3132](https://github.com/lancedb/lancedb/issues/3132))?

### 4. Embeddings

Should `SourceField` on blob columns resolve to bytes automatically, or stay explicit?

### 5. Cloud

Sketch REST now for parallel work, or after local ships?

### 6. `take_blobs` verb

`take_blobs` mirrors Lance `Dataset::take_blobs` and the `take_row_ids` family, which is why the doc leads with it. But `take` is an Arrow/Lance internal verb; no vector DB exposes it for retrieval (Pinecone `fetch`, Milvus `get`). Rename the user-facing call to `fetch_blobs(hits, column="image")` (rows-first, fetch vocabulary) for a cleaner external surface, at the cost of diverging from the Lance name and the `take_row_ids` family? The Rust-layer `take_blobs(column, &[u64])` stays regardless; this is only about the binding-facing name.

## Technical constraints (implementation)

These are design requirements for implementers.

- **Thin wrapper over Lance Blob V2** (format ≥ 2.2): `blob()` factory, byte coercion on `add`, `take_blobs` / `take_blob_files` on local tables, `NotSupported` on remote until REST exists.
- **Python schema field must match the engine's Lance version** rather than the standalone `pylance` package: layouts can differ (e.g. extra struct fields in older pylance). `lancedb.blob()` should build the field the pinned `lance` crate expects.
- **Null alignment:** Lance `take_blobs` may omit null rows; LanceDB `take_blobs` must stitch nulls back so the output stays aligned with what the user asked for.
- **Table ABC parity:** `take_blobs`, `take_blob_files`, and `blob_columns` belong on the `Table` ABC like `take_row_ids`, with `NotSupported` stubs on `RemoteTable` until the REST API lands. The prototype put them on `LanceTable` only.
- **Sequencing** per issue [#3231](https://github.com/lancedb/lancedb/issues/3231) §4: inline write + descriptors + `take_blobs` ship first across Rust and Python; URI-backed reads, remote materialization, TypeScript parity, and pre-signed URLs follow in later iterations.

Related: [#3210](https://github.com/lancedb/lancedb/issues/3210), [lance#6938](https://github.com/lance-format/lance/issues/6938), [lance#6322](https://github.com/lance-format/lance/issues/6322). Lance reference: [blob.rs](https://github.com/lance-format/lance/blob/main/rust/lance/src/blob.rs), [take_blobs](https://github.com/lance-format/lance/blob/main/rust/lance/src/dataset.rs).
