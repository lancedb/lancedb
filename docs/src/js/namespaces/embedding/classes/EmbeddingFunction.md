[**@lancedb/lancedb**](../../../README.md) • **Docs**

***

[@lancedb/lancedb](../../../globals.md) / [embedding](../README.md) / EmbeddingFunction

# Class: `abstract` EmbeddingFunction&lt;T, M&gt;

An embedding function that automatically creates vector representation for a given column.

It's important subclasses pass the **original** options to the super constructor
and then pass those options to `resolveConfig` to resolve any variables before
using them.

## Example

```ts
class MyEmbeddingFunction extends EmbeddingFunction {
  constructor(options: {model: string, timeout: number}) {
    super(optionsRaw);
    const options = this.resolveConfig(optionsRaw);
    this.model = options.model;
    this.timeout = options.timeout;
  }
}
```

## Extended by

- [`TextEmbeddingFunction`](TextEmbeddingFunction.md)

## Type Parameters

• **T** = `any`

• **M** *extends* [`FunctionOptions`](../interfaces/FunctionOptions.md) = [`FunctionOptions`](../interfaces/FunctionOptions.md)

## Constructors

### new EmbeddingFunction()

```ts
new EmbeddingFunction<T, M>(args): EmbeddingFunction<T, M>
```

#### Parameters

* **args**: `Partial`&lt;`M`&gt; = `{}`

#### Returns

[`EmbeddingFunction`](EmbeddingFunction.md)&lt;`T`, `M`&gt;

## Methods

### computeQueryEmbeddings()

```ts
computeQueryEmbeddings(data): Promise<number[] | Float32Array | Float64Array>
```

Compute the embeddings for a single query

#### Parameters

* **data**: `T`

#### Returns

`Promise`&lt;`number`[] \| `Float32Array` \| `Float64Array`&gt;

***

### computeSourceEmbeddings()

```ts
abstract computeSourceEmbeddings(data): Promise<number[][] | Float32Array[] | Float64Array[]>
```

Creates a vector representation for the given values.

#### Parameters

* **data**: `T`[]

#### Returns

`Promise`&lt;`number`[][] \| `Float32Array`[] \| `Float64Array`[]&gt;

***

### embeddingDataType()

```ts
abstract embeddingDataType(): Float<Floats>
```

The datatype of the embeddings

#### Returns

`Float`&lt;`Floats`&gt;

***

### getSensitiveKeys()

```ts
abstract protected getSensitiveKeys(): string[]
```

Provide a list of keys in the function options that should be treated as
sensitive. If users pass raw values for these keys, they will be rejected.

#### Returns

`string`[]

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

***

### ndims()

```ts
ndims(): undefined | number
```

The number of dimensions of the embeddings

#### Returns

`undefined` \| `number`

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

***

### toJSON()

```ts
toJSON(): Record<string, any>
```

Get the original arguments to the constructor, to serialize them so they
can be used to recreate the embedding function later.

#### Returns

`Record`&lt;`string`, `any`&gt;

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
