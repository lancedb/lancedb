[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / blobWithRange

# Function: blobWithRange()

```ts
function blobWithRange(name, options): Field
```

Declares a blob column that also accepts external byte ranges.

Adds nullable `position` and `size` UInt64 fields to the layout of [blob](blob.md).
Set both to select `[position, position + size)` within `uri`, or leave both
null or omitted to reference the whole file. `size` must be positive.
Inline bytes and null blobs are accepted too. External URIs must be within a
registered base, unless a local append explicitly allows otherwise.

## Parameters

* **name**: `string`

* **options**: [`BlobOptions`](../type-aliases/BlobOptions.md) = `{}`

## Returns

`Field`

## Example

```ts
import { Schema } from "apache-arrow";
import { blobWithRange, connect } from "@lancedb/lancedb";

const db = await connect("./data");
const table = await db.createEmptyTable(
  "clips", new Schema([blobWithRange("clip")]),
);
await table.add(
  [{ clip: { uri: "file:///data/video.bin", position: 4, size: 6 } }],
  { allowExternalBlobOutsideBases: true },
);
const rows = await table.query().withRowId().toArray();
const bytes = await table.fetchBlobs("clip", [rows[0]._rowid as bigint]);
```
