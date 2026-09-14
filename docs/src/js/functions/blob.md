[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / blob

# Function: blob()

```ts
function blob(name, options): Field
```

Declares a `lance.blob.v2` column.

Query results are descriptors, not payload bytes. Use [Table.fetchBlobs](../classes/Table.md#fetchblobs)
or [Table.fetchBlobFiles](../classes/Table.md#fetchblobfiles) to read bytes.

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
