[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / QueryExecutionOptions

# Interface: QueryExecutionOptions

Options that control the behavior of a particular query execution

## Properties

### blobMode?

```ts
optional blobMode: BlobMode;
```

How to return blob v2 columns. Applies to `toArrow()` and `toArray()`.

- `"descriptions"` (default): each blob is a descriptor. Read the bytes
  with [Table.fetchBlobs](../classes/Table.md#fetchblobs) or [Table.fetchBlobFiles](../classes/Table.md#fetchblobfiles).
- `"bytes"`: each top-level blob column holds the blob bytes as
  `LargeBinary`. The query also reads `_rowid` to fetch the bytes, and
  leaves it out of the result unless [QueryBase.withRowId](../classes/QueryBase.md#withrowid) was
  called. All the bytes are held in memory.

Blobs nested in a struct or a list keep their descriptors.

The rows and the bytes come from the same table version. If the table
changes while the query runs, the query runs again, up to three times.

#### Example

```ts
const rows = await table
  .query()
  .select(["id", "image"])
  .toArray({ blobMode: "bytes" });
const image: Uint8Array | null = rows[0].image;
```

***

### maxBatchLength?

```ts
optional maxBatchLength: number;
```

The maximum number of rows to return in a single batch

Batches may have fewer rows if the underlying data is stored
in smaller chunks.

***

### timeoutMs?

```ts
optional timeoutMs: number;
```

Timeout for query execution in milliseconds
