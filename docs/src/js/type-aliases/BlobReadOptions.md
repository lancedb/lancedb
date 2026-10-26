[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / BlobReadOptions

# Type Alias: BlobReadOptions

```ts
type BlobReadOptions: object;
```

Options for blob reads.

## Type declaration

### signal?

```ts
optional signal: AbortSignal;
```

Cancels the read. The call rejects with `signal.reason`, and the native
read stops, including in-flight requests to a remote table.
