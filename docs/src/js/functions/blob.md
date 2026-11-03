[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / blob

# Function: blob()

```ts
function blob(name, options): Field
```

Declares a `lance.blob.v2` column.
Uses nullable `data`, `uri`, `position`, and `size` fields for inline bytes,
whole external files, and external byte ranges. Set both `position` and
positive `size` for a range, or omit both for a whole file.

Existing two-field columns remain writable. Appends and merge inserts fill
missing range fields with nulls for four-field targets and drop all-null
range fields for two-field targets. Non-null ranges require a four-field
target. Overwrite with Arrow input replaces the table's schema; row objects
are converted using the current table schema.

Query results are descriptors, not payload bytes. Use [Table.fetchBlobs](../classes/Table.md#fetchblobs)
or [Table.fetchBlobFiles](../classes/Table.md#fetchblobfiles) to read bytes.
Blobs inside lists can be written and queried as descriptors, but cannot
currently be fetched because a row ID does not identify a list element.

## Parameters

* **name**: `string`

* **options**: [`BlobOptions`](../type-aliases/BlobOptions.md) = `{}`

## Returns

`Field`

## Example

```ts
import { readFile } from "node:fs/promises";
import { Field, Int64, Schema } from "apache-arrow";
import { blob, connect } from "@lancedb/lancedb";

const db = await connect("./data");
const video = await readFile("clip.mp4");
const table = await db.createTable(
  "videos",
  [{ id: 1n, video }],
  {
    schema: new Schema([
      new Field("id", new Int64()),
      blob("video"),
    ]),
  },
);

const rows = await table.query().select(["id"]).withRowId().toArray();
const rowIds = rows.map((row) => row._rowid as bigint);
const bytes = await table.fetchBlobs("video", rowIds);

const [handle] = await table.fetchBlobFiles("video", rowIds);
const size = handle!.size();
const header = await handle!.readRange(0n, size < 65536n ? size : 65536n);
```
