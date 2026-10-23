[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / BlobFile

# Class: BlobFile

A lazy handle to blob bytes. Create one with [Table.fetchBlobFiles](Table.md#fetchblobfiles).

## Methods

### close()

```ts
close(): Promise<void>
```

Releases the handle. Reads after `close()` fail. Calling it again does
nothing.

#### Returns

`Promise`&lt;`void`&gt;

***

### isClosed()

```ts
isClosed(): boolean
```

Returns true after [BlobFile.close](BlobFile.md#close).

#### Returns

`boolean`

***

### read()

```ts
read(maxBytes?, options?): Promise<Buffer>
```

Reads from the cursor and advances the cursor.

Reads to the end when `maxBytes` is omitted, or at most `maxBytes` bytes
otherwise. Returns an empty buffer at the end of the blob.
[BlobFile.readRange](BlobFile.md#readrange) does not move the cursor.

#### Parameters

* **maxBytes?**: `bigint`

* **options?**: [`BlobReadOptions`](../type-aliases/BlobReadOptions.md)

#### Returns

`Promise`&lt;`Buffer`&gt;

***

### readRange()

```ts
readRange(
   start,
   end,
   options?): Promise<Buffer>
```

Reads the half-open byte range `[start, end)`.

Fails when `end` is past the blob size. Does not move the cursor.

#### Parameters

* **start**: `bigint`

* **end**: `bigint`

* **options?**: [`BlobReadOptions`](../type-aliases/BlobReadOptions.md)

#### Returns

`Promise`&lt;`Buffer`&gt;

***

### readRanges()

```ts
readRanges(ranges, options?): Promise<Buffer[]>
```

Reads several half-open byte ranges. Returns one buffer per range, in
the order given.

Fails when any `end` is past the blob size. Does not move the cursor.

#### Parameters

* **ranges**: [`BlobRange`](../type-aliases/BlobRange.md)[]

* **options?**: [`BlobReadOptions`](../type-aliases/BlobReadOptions.md)

#### Returns

`Promise`&lt;`Buffer`[]&gt;

***

### seek()

```ts
seek(position): Promise<void>
```

Moves the cursor to `position`, in bytes from the start of the blob.

#### Parameters

* **position**: `bigint`

#### Returns

`Promise`&lt;`void`&gt;

***

### size()

```ts
size(): bigint
```

Returns the blob size in bytes.

#### Returns

`bigint`

***

### tell()

```ts
tell(): Promise<bigint>
```

Returns the cursor position in bytes.

#### Returns

`Promise`&lt;`bigint`&gt;
