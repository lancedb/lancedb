[**@lancedb/lancedb**](../../../README.md) • **Docs**

***

[@lancedb/lancedb](../../../globals.md) / [embedding](../README.md) / getRegistry

# Function: getRegistry()

> **getRegistry**(): [`EmbeddingFunctionRegistry`](../classes/EmbeddingFunctionRegistry.md)

Utility function to get the global instance of the registry

## Returns

[`EmbeddingFunctionRegistry`](../classes/EmbeddingFunctionRegistry.md)

`EmbeddingFunctionRegistry` The global instance of the registry

## Example

```ts
const registry = getRegistry();
const openai = registry.get("openai").create();
