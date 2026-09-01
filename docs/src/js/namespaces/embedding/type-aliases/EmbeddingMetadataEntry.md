[**@lancedb/lancedb**](../../../README.md) • **Docs**

***

[@lancedb/lancedb](../../../globals.md) / [embedding](../README.md) / EmbeddingMetadataEntry

# Type Alias: EmbeddingMetadataEntry

```ts
type EmbeddingMetadataEntry: object;
```

One entry of the `embedding_functions` schema metadata, with the column
keys normalized across the bindings' spellings.

## Type declaration

### model

```ts
model: EmbeddingFunction["TOptions"];
```

### name

```ts
name: string;
```

### sourceColumn

```ts
sourceColumn: string;
```

### vectorColumn

```ts
vectorColumn: string;
```
