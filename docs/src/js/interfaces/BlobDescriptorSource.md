[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / BlobDescriptorSource

# Interface: BlobDescriptorSource

A source-bearing Blob V2 descriptor returned by the SQL `blob_descriptor`
function. Field names match the descriptor object/Arrow struct shape.

## Properties

### branch?

```ts
optional branch: null | string;
```

***

### column

```ts
column: string;
```

***

### committed\_at\_micros

```ts
committed_at_micros: number | bigint;
```

***

### row\_is\_address

```ts
row_is_address: boolean;
```

***

### row\_reference

```ts
row_reference: number | bigint;
```

***

### uri

```ts
uri: string;
```

***

### version

```ts
version: number | bigint;
```
