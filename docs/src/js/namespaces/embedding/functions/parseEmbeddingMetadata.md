[**@lancedb/lancedb**](../../../README.md) • **Docs**

***

[@lancedb/lancedb](../../../globals.md) / [embedding](../README.md) / parseEmbeddingMetadata

# Function: parseEmbeddingMetadata()

```ts
function parseEmbeddingMetadata(json): EmbeddingMetadataEntry[]
```

The single parser for `embedding_functions` schema metadata: every reader
goes through here, so the wire contract cannot fork between them.

## Parameters

* **json**: `string`

## Returns

[`EmbeddingMetadataEntry`](../type-aliases/EmbeddingMetadataEntry.md)[]
