[**@lancedb/lancedb**](../../../README.md) • **Docs**

***

[@lancedb/lancedb](../../../globals.md) / [embedding](../README.md) / RegisterOptions

# Interface: RegisterOptions

Options for [EmbeddingFunctionRegistry.register](../classes/EmbeddingFunctionRegistry.md#register).

## Properties

### safeToRestore?

```ts
optional safeToRestore: boolean;
```

Whether table metadata may configure this function without approval.

Only set this if the class's static `loadedModels` and `endpoints` methods
report every model and URL its options can make it load or contact. Those
still need approval. Otherwise, a function restored from table metadata
only computes embeddings once its options are approved, see
[EmbeddingFunctionRegistry.allowFunction](../classes/EmbeddingFunctionRegistry.md#allowfunction).
