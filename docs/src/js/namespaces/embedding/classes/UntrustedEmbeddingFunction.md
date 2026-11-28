[**@lancedb/lancedb**](../../../README.md) • **Docs**

***

[@lancedb/lancedb](../../../globals.md) / [embedding](../README.md) / UntrustedEmbeddingFunction

# Class: UntrustedEmbeddingFunction

Stands in for an embedding function restored from table metadata whose
options aren't approved yet. Computing embeddings throws
[UntrustedEmbeddingConfigError](UntrustedEmbeddingConfigError.md), which says how to approve them.
Writing the table's metadata again keeps the original options.

## Extends

- [`EmbeddingFunction`](EmbeddingFunction.md)

## Constructors

### new UntrustedEmbeddingFunction()

```ts
new UntrustedEmbeddingFunction(
   functionName,
   options,
   reasons): UntrustedEmbeddingFunction
```

#### Parameters

* **functionName**: `string`

* **options**: [`FunctionOptions`](../interfaces/FunctionOptions.md)

* **reasons**: `string`[]

#### Returns

[`UntrustedEmbeddingFunction`](UntrustedEmbeddingFunction.md)

#### Overrides

[`EmbeddingFunction`](EmbeddingFunction.md).[`constructor`](EmbeddingFunction.md#constructors)

## Properties

### functionName

```ts
readonly functionName: string;
```

***

### reasons

```ts
readonly reasons: string[];
```

## Methods

### computeQueryEmbeddings()

```ts
computeQueryEmbeddings(): Promise<Float32Array | Float64Array | Uint8Array | number[]>
```

Compute the embeddings for a single query

#### Returns

`Promise`&lt;`Float32Array` \| `Float64Array` \| `Uint8Array` \| `number`[]&gt;

#### Overrides

[`EmbeddingFunction`](EmbeddingFunction.md).[`computeQueryEmbeddings`](EmbeddingFunction.md#computequeryembeddings)

***

### computeSourceEmbeddings()

```ts
computeSourceEmbeddings(): Promise<number[][]>
```

Creates a vector representation for the given values.

#### Returns

`Promise`&lt;`number`[][]&gt;

#### Overrides

[`EmbeddingFunction`](EmbeddingFunction.md).[`computeSourceEmbeddings`](EmbeddingFunction.md#computesourceembeddings)

***

### embeddingDataType()

```ts
embeddingDataType(): Float<Floats>
```

The datatype of the embeddings

#### Returns

`Float`&lt;`Floats`&gt;

#### Overrides

[`EmbeddingFunction`](EmbeddingFunction.md).[`embeddingDataType`](EmbeddingFunction.md#embeddingdatatype)

***

### getSensitiveKeys()

```ts
protected getSensitiveKeys(): string[]
```

Provide a list of keys in the function options that should be treated as
sensitive. If users pass raw values for these keys, they will be rejected.

#### Returns

`string`[]

#### Inherited from

[`EmbeddingFunction`](EmbeddingFunction.md).[`getSensitiveKeys`](EmbeddingFunction.md#getsensitivekeys)

***

### init()?

```ts
optional init(): Promise<void>
```

Optionally load any resources needed for the embedding function.

This method is called after the embedding function has been initialized
but before any embeddings are computed. It is useful for loading local models
or other resources that are needed for the embedding function to work.

#### Returns

`Promise`&lt;`void`&gt;

#### Inherited from

[`EmbeddingFunction`](EmbeddingFunction.md).[`init`](EmbeddingFunction.md#init)

***

### ndims()

```ts
ndims(): undefined | number
```

The number of dimensions of the embeddings

#### Returns

`undefined` \| `number`

#### Inherited from

[`EmbeddingFunction`](EmbeddingFunction.md).[`ndims`](EmbeddingFunction.md#ndims)

***

### resolveVariables()

```ts
protected resolveVariables(config): Partial<FunctionOptions>
```

Apply variables to the config.

Secret variables, the default for
`EmbeddingFunctionRegistry.setVar`, only resolve in the keys
returned by [getSensitiveKeys](UntrustedEmbeddingFunction.md#getsensitivekeys).

#### Parameters

* **config**: `Partial`&lt;[`FunctionOptions`](../interfaces/FunctionOptions.md)&gt;

#### Returns

`Partial`&lt;[`FunctionOptions`](../interfaces/FunctionOptions.md)&gt;

#### Inherited from

[`EmbeddingFunction`](EmbeddingFunction.md).[`resolveVariables`](EmbeddingFunction.md#resolvevariables)

***

### sourceField()

```ts
sourceField(optionsOrDatatype): [DataType<Type, any>, Map<string, EmbeddingFunction<any, FunctionOptions>>]
```

sourceField is used in combination with `LanceSchema` to provide a declarative data model

#### Parameters

* **optionsOrDatatype**: `DataType`&lt;`Type`, `any`&gt; \| `Partial`&lt;[`FieldOptions`](../interfaces/FieldOptions.md)&lt;`DataType`&lt;`Type`, `any`&gt;&gt;&gt;
    The options for the field or the datatype

#### Returns

[`DataType`&lt;`Type`, `any`&gt;, `Map`&lt;`string`, [`EmbeddingFunction`](EmbeddingFunction.md)&lt;`any`, [`FunctionOptions`](../interfaces/FunctionOptions.md)&gt;&gt;]

#### See

[LanceSchema](../functions/LanceSchema.md)

#### Inherited from

[`EmbeddingFunction`](EmbeddingFunction.md).[`sourceField`](EmbeddingFunction.md#sourcefield)

***

### toJSON()

```ts
toJSON(): Record<string, any>
```

Get the original arguments to the constructor, to serialize them so they
can be used to recreate the embedding function later.

#### Returns

`Record`&lt;`string`, `any`&gt;

#### Overrides

[`EmbeddingFunction`](EmbeddingFunction.md).[`toJSON`](EmbeddingFunction.md#tojson)

***

### vectorField()

```ts
vectorField(optionsOrDatatype?): [DataType<Type, any>, Map<string, EmbeddingFunction<any, FunctionOptions>>]
```

vectorField is used in combination with `LanceSchema` to provide a declarative data model

#### Parameters

* **optionsOrDatatype?**: `DataType`&lt;`Type`, `any`&gt; \| `Partial`&lt;[`FieldOptions`](../interfaces/FieldOptions.md)&lt;`DataType`&lt;`Type`, `any`&gt;&gt;&gt;
    The options for the field

#### Returns

[`DataType`&lt;`Type`, `any`&gt;, `Map`&lt;`string`, [`EmbeddingFunction`](EmbeddingFunction.md)&lt;`any`, [`FunctionOptions`](../interfaces/FunctionOptions.md)&gt;&gt;]

#### See

[LanceSchema](../functions/LanceSchema.md)

#### Inherited from

[`EmbeddingFunction`](EmbeddingFunction.md).[`vectorField`](EmbeddingFunction.md#vectorfield)
