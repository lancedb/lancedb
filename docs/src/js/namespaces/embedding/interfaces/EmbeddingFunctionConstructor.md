[**@lancedb/lancedb**](../../../README.md) • **Docs**

***

[@lancedb/lancedb](../../../globals.md) / [embedding](../README.md) / EmbeddingFunctionConstructor

# Interface: EmbeddingFunctionConstructor&lt;T&gt;

## Type Parameters

• **T** *extends* [`EmbeddingFunction`](../classes/EmbeddingFunction.md) = [`EmbeddingFunction`](../classes/EmbeddingFunction.md)

## Constructors

### new EmbeddingFunctionConstructor()

```ts
new EmbeddingFunctionConstructor(modelOptions?): T
```

#### Parameters

* **modelOptions?**: `T`\[`"TOptions"`\]

#### Returns

`T`

## Methods

### endpoints()?

```ts
optional endpoints(options): string[]
```

The custom endpoints these options make the function send data to. A
function restored from table metadata only uses them once they are
approved with `EmbeddingFunctionRegistry.allowEndpoint`.

It is called before the constructor, with the non-sensitive options.

#### Parameters

* **options**: [`FunctionOptions`](FunctionOptions.md)

#### Returns

`string`[]

***

### loadedModels()?

```ts
optional loadedModels(options): [string, undefined | string][]
```

The models these options make the function load, other than its default
model, as `[name, revision]` pairs. A function restored from table
metadata only loads them once they are approved with
`EmbeddingFunctionRegistry.allowModel`.

It is called before the constructor, with the non-sensitive options.

#### Parameters

* **options**: [`FunctionOptions`](FunctionOptions.md)

#### Returns

[`string`, `undefined` \| `string`][]
