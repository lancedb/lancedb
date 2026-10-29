[**@lancedb/lancedb**](../../../README.md) • **Docs**

***

[@lancedb/lancedb](../../../globals.md) / [embedding](../README.md) / getRegistry

# Function: getRegistry()

```ts
function getRegistry(): EmbeddingFunctionRegistry
```

Get the global embedding function registry.

LanceDB built-in providers are initialized when this public API is first
used, so importing the root package does not change automatic search
selection for tables without embedding metadata.

## Returns

[`EmbeddingFunctionRegistry`](../classes/EmbeddingFunctionRegistry.md)
