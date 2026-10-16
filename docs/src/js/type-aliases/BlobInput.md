[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / BlobInput

# Type Alias: BlobInput

```ts
type BlobInput:
  | BlobData
  | BlobUri
  | object
  | object
  | null
  | undefined;
```

A value accepted for a `lance.blob.v2` column (see [blob](../functions/blob.md)).

Either inline bytes, a URI pointing at external bytes, or a struct that sets
exactly one of `data` or `uri`. `null` and `undefined` write a null blob.

## Example

```ts
import { pathToFileURL } from "node:url";
import type { BlobInput } from "@lancedb/lancedb";

const rows: { id: bigint; image: BlobInput }[] = [
  { id: 1n, image: await (await fetch(url)).arrayBuffer() },
  { id: 2n, image: pathToFileURL("/data/cat.png") },
  { id: 3n, image: { data: new Blob(["hello"]) } },
];
await table.add(rows);
```
