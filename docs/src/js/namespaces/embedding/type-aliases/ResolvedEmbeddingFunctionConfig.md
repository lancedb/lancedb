[**@lancedb/lancedb**](../../../README.md) • **Docs**

***

[@lancedb/lancedb](../../../globals.md) / [embedding](../README.md) / ResolvedEmbeddingFunctionConfig

# Type Alias: ResolvedEmbeddingFunctionConfig

```ts
type ResolvedEmbeddingFunctionConfig: EmbeddingFunctionConfig & object;
```

An [EmbeddingFunctionConfig] read back from table metadata, where the
vector column is always recorded.

## Type declaration

### vectorColumn

```ts
vectorColumn: string;
```
