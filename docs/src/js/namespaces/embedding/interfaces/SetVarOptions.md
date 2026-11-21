[**@lancedb/lancedb**](../../../README.md) • **Docs**

***

[@lancedb/lancedb](../../../globals.md) / [embedding](../README.md) / SetVarOptions

# Interface: SetVarOptions

Options for [EmbeddingFunctionRegistry.setVar](../classes/EmbeddingFunctionRegistry.md#setvar).

## Properties

### secret?

```ts
optional secret: boolean;
```

Whether the value is a secret, such as an API key. Defaults to true.

Secret variables can only be used in an embedding function's sensitive
keys, which send them only as credentials. Anyone who can write a table
chooses the variable names in its embedding options, so a secret in any
other option, such as a model name, could be shown to them in an error
message. Pass `secret: false` for settings such as a model name.
