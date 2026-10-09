[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / BlobData

# Type Alias: BlobData

```ts
type BlobData: Buffer | Uint8Array | ArrayBuffer | Blob;
```

Bytes accepted for a blob value. `ArrayBuffer` is wrapped without copying.
`Blob` and `File` are read with `arrayBuffer()`, which only the async write
paths ([Connection.createTable](../classes/Connection.md#createtable), [Table.add](../classes/Table.md#add),
[Table.mergeInsert](../classes/Table.md#mergeinsert)) can do.
