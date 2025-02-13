[**@lancedb/lancedb**](../../../README.md) • **Docs**

***

[@lancedb/lancedb](../../../globals.md) / [embedding](../README.md) / TextEmbeddingFunction

# Class: `abstract` TextEmbeddingFunction&lt;M&gt;

an abstract class for implementing embedding functions that take text as input

## Extends

- [`EmbeddingFunction`](EmbeddingFunction.md)&lt;`string`, `M`&gt;

## Type Parameters

• **M** *extends* [`FunctionOptions`](../interfaces/FunctionOptions.md) = [`FunctionOptions`](../interfaces/FunctionOptions.md)

## Constructors

### new TextEmbeddingFunction()

```ts
new TextEmbeddingFunction<M>(args): TextEmbeddingFunction<M>
```

#### Parameters

* **args**: `Partial`&lt;`M`&gt; = `{}`

#### Returns

[`TextEmbeddingFunction`](TextEmbeddingFunction.md)&lt;`M`&gt;

#### Inherited from

[`EmbeddingFunction`](EmbeddingFunction.md).[`constructor`](EmbeddingFunction.md#constructors)

## Methods

### computeQueryEmbeddings()

```ts
computeQueryEmbeddings(data): Promise<number[] | Float32Array | Float64Array>
```

Compute the embeddings for a single query

#### Parameters

* **data**: `string`

#### Returns

`Promise`&lt;`number`[] \| `Float32Array` \| `Float64Array`&gt;

#### Overrides

[`EmbeddingFunction`](EmbeddingFunction.md).[`computeQueryEmbeddings`](EmbeddingFunction.md#computequeryembeddings)

***

### computeSourceEmbeddings()

```ts
computeSourceEmbeddings(data): Promise<number[][] | Float32Array[] | Float64Array[]>
```

Creates a vector representation for the given values.

#### Parameters

* **data**: `string`[]

#### Returns

`Promise`&lt;`number`[][] \| `Float32Array`[] \| `Float64Array`[]&gt;

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

### generateEmbeddings()

```ts
abstract generateEmbeddings(texts, ...args): Promise<number[][] | Float32Array[] | Float64Array[]>
```

#### Parameters

* **texts**: `string`[]

* ...**args**: `any`[]

#### Returns

`Promise`&lt;`number`[][] \| `Float32Array`[] \| `Float64Array`[]&gt;

***

### getSensitiveKeys()

```ts
abstract protected getSensitiveKeys(): string[]
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
protected resolveVariables<T>(config): Partial<T>
```

Apply variables to the config.

#### Type Parameters

• **T** *extends* [`FunctionOptions`](../interfaces/FunctionOptions.md)

#### Parameters

* **config**: `Partial`&lt;`T`&gt;

#### Returns

`Partial`&lt;`T`&gt;

#### Inherited from

[`EmbeddingFunction`](EmbeddingFunction.md).[`resolveVariables`](EmbeddingFunction.md#resolvevariables)

***

### sourceField()

```ts
sourceField(): [DataType<Type, any>, Map<string, EmbeddingFunction<any, FunctionOptions>>]
```

sourceField is used in combination with `LanceSchema` to provide a declarative data model

#### Returns

[`DataType`&lt;`Type`, `any`&gt;, `Map`&lt;`string`, [`EmbeddingFunction`](EmbeddingFunction.md)&lt;`any`, [`FunctionOptions`](../interfaces/FunctionOptions.md)&gt;&gt;]

#### See

[LanceSchema](../functions/LanceSchema.md)

#### Overrides

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

#### Inherited from

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
