[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / BlobDescriptor

# Interface: BlobDescriptor

A source-bearing Blob V2 descriptor accepted by [Table.openBlobFile](../classes/Table.md#openblobfile).
Query results expose these fields as an Arrow struct. The source pins the
table version, branch, column and row identity used to open the handle.

## Properties

### blob\_id

```ts
blob_id: number;
```

***

### blob\_uri

```ts
blob_uri: string;
```

***

### kind

```ts
kind: number;
```

***

### position

```ts
position: number | bigint;
```

***

### size

```ts
size: number | bigint;
```

***

### source

```ts
source: BlobDescriptorSource;
```
