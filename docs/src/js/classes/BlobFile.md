[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / BlobFile

# Class: BlobFile

A lazy handle to blob bytes. Create one with [Table.fetchBlobFiles](Table.md#fetchblobfiles).

## Methods

### read()

```ts
read(): Promise<Buffer>
```

Reads from the cursor to the end and advances the cursor.

A second call returns an empty buffer. [BlobFile.readRange](BlobFile.md#readrange) does
not move the cursor.

#### Returns

`Promise`&lt;`Buffer`&gt;

***

### readRange()

```ts
readRange(start, end): Promise<Buffer>
```

Reads the half-open byte range `[start, end)`.

Fails when `end` is past the blob size. Does not move the cursor.

#### Parameters

* **start**: `bigint`

* **end**: `bigint`

#### Returns

`Promise`&lt;`Buffer`&gt;

***

### size()

```ts
size(): bigint
```

Returns the blob size in bytes.

#### Returns

`bigint`
