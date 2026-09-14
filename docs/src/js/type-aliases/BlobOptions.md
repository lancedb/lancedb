[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / BlobOptions

# Type Alias: BlobOptions

```ts
type BlobOptions: object;
```

## Type declaration

### dedicatedSizeThreshold?

```ts
optional dedicatedSizeThreshold: number;
```

Max payload bytes stored in a packed sidecar before a dedicated file. Must
be a positive safe integer.

### inlineSizeThreshold?

```ts
optional inlineSizeThreshold: number;
```

Max payload bytes kept inline in the data file. Zero is allowed. Must be a
safe integer.

### nullable?

```ts
optional nullable: boolean;
```

Defaults to true.

### packFileSizeThreshold?

```ts
optional packFileSizeThreshold: number;
```

Max bytes in one packed sidecar before starting another. Must be a positive
safe integer.
