[**@lancedb/lancedb**](../../../README.md) • **Docs**

***

[@lancedb/lancedb](../../../globals.md) / [embedding](../README.md) / EmbeddingFunction

# Class: `abstract` EmbeddingFunction&lt;T, M&gt;

An embedding function that automatically creates vector representation for a given column.

## Extended by

- [`OpenAIEmbeddingFunction`](OpenAIEmbeddingFunction.md)

## Type Parameters

• **T** = `any`

• **M** *extends* `FunctionOptions` = `FunctionOptions`

## Constructors

### new EmbeddingFunction()

> **new EmbeddingFunction**&lt;`T`, `M`&gt;(): [`EmbeddingFunction`](EmbeddingFunction.md)&lt;`T`, `M`&gt;

#### Returns

[`EmbeddingFunction`](EmbeddingFunction.md)&lt;`T`, `M`&gt;

## Methods

### computeQueryEmbeddings()

> **computeQueryEmbeddings**(`data`): `Promise`&lt;`number`[] \| `Float32Array` \| `Float64Array`&gt;

Compute the embeddings for a single query

#### Parameters

• **data**: `T`

#### Returns

`Promise`&lt;`number`[] \| `Float32Array` \| `Float64Array`&gt;

***

### computeSourceEmbeddings()

> `abstract` **computeSourceEmbeddings**(`data`): `Promise`&lt;`number`[][] \| `Float32Array`[] \| `Float64Array`[]&gt;

Creates a vector representation for the given values.

#### Parameters

• **data**: `T`[]

#### Returns

`Promise`&lt;`number`[][] \| `Float32Array`[] \| `Float64Array`[]&gt;

***

### embeddingDataType()

> `abstract` **embeddingDataType**(): `Float`&lt;`Floats`&gt;

The datatype of the embeddings

#### Returns

`Float`&lt;`Floats`&gt;

***

### ndims()

> **ndims**(): `undefined` \| `number`

The number of dimensions of the embeddings

#### Returns

`undefined` \| `number`

***

### sourceField()

> **sourceField**(`optionsOrDatatype`): [`DataType`&lt;`Type`, `any`&gt;, `Map`&lt;`string`, [`EmbeddingFunction`](EmbeddingFunction.md)&lt;`any`, `FunctionOptions`&gt;&gt;]

sourceField is used in combination with `LanceSchema` to provide a declarative data model

#### Parameters

• **optionsOrDatatype**: `DataType`&lt;`Type`, `any`&gt; \| `Partial`&lt;`FieldOptions`&lt;`DataType`&lt;`Type`, `any`&gt;&gt;&gt;

The options for the field or the datatype

#### Returns

[`DataType`&lt;`Type`, `any`&gt;, `Map`&lt;`string`, [`EmbeddingFunction`](EmbeddingFunction.md)&lt;`any`, `FunctionOptions`&gt;&gt;]

#### See

lancedb.LanceSchema

***

### toJSON()

> `abstract` **toJSON**(): `Partial`&lt;`M`&gt;

Convert the embedding function to a JSON object
It is used to serialize the embedding function to the schema
It's important that any object returned by this method contains all the necessary
information to recreate the embedding function

It should return the same object that was passed to the constructor
If it does not, the embedding function will not be able to be recreated, or could be recreated incorrectly

#### Returns

`Partial`&lt;`M`&gt;

#### Example

```ts
class MyEmbeddingFunction extends EmbeddingFunction {
  constructor(options: {model: string, timeout: number}) {
    super();
    this.model = options.model;
    this.timeout = options.timeout;
  }
  toJSON() {
    return {
      model: this.model,
      timeout: this.timeout,
    };
}
```

***

### vectorField()

> **vectorField**(`optionsOrDatatype`?): [`DataType`&lt;`Type`, `any`&gt;, `Map`&lt;`string`, [`EmbeddingFunction`](EmbeddingFunction.md)&lt;`any`, `FunctionOptions`&gt;&gt;]

vectorField is used in combination with `LanceSchema` to provide a declarative data model

#### Parameters

• **optionsOrDatatype?**: `DataType`&lt;`Type`, `any`&gt; \| `Partial`&lt;`FieldOptions`&lt;`DataType`&lt;`Type`, `any`&gt;&gt;&gt;

#### Returns

[`DataType`&lt;`Type`, `any`&gt;, `Map`&lt;`string`, [`EmbeddingFunction`](EmbeddingFunction.md)&lt;`any`, `FunctionOptions`&gt;&gt;]

#### See

lancedb.LanceSchema
