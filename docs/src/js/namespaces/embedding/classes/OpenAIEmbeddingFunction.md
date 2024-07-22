[**@lancedb/lancedb**](../../../README.md) • **Docs**

***

[@lancedb/lancedb](../../../globals.md) / [embedding](../README.md) / OpenAIEmbeddingFunction

# Class: OpenAIEmbeddingFunction

An embedding function that automatically creates vector representation for a given column.

## Extends

- [`EmbeddingFunction`](EmbeddingFunction.md)&lt;`string`, `Partial`&lt;[`OpenAIOptions`](../type-aliases/OpenAIOptions.md)&gt;&gt;

## Constructors

### new OpenAIEmbeddingFunction()

> **new OpenAIEmbeddingFunction**(`options`): [`OpenAIEmbeddingFunction`](OpenAIEmbeddingFunction.md)

#### Parameters

• **options**: `Partial`&lt;[`OpenAIOptions`](../type-aliases/OpenAIOptions.md)&gt; = `...`

#### Returns

[`OpenAIEmbeddingFunction`](OpenAIEmbeddingFunction.md)

#### Overrides

[`EmbeddingFunction`](EmbeddingFunction.md).[`constructor`](EmbeddingFunction.md#constructors)

## Methods

### computeQueryEmbeddings()

> **computeQueryEmbeddings**(`data`): `Promise`&lt;`number`[]&gt;

Compute the embeddings for a single query

#### Parameters

• **data**: `string`

#### Returns

`Promise`&lt;`number`[]&gt;

#### Overrides

[`EmbeddingFunction`](EmbeddingFunction.md).[`computeQueryEmbeddings`](EmbeddingFunction.md#computequeryembeddings)

***

### computeSourceEmbeddings()

> **computeSourceEmbeddings**(`data`): `Promise`&lt;`number`[][]&gt;

Creates a vector representation for the given values.

#### Parameters

• **data**: `string`[]

#### Returns

`Promise`&lt;`number`[][]&gt;

#### Overrides

[`EmbeddingFunction`](EmbeddingFunction.md).[`computeSourceEmbeddings`](EmbeddingFunction.md#computesourceembeddings)

***

### embeddingDataType()

> **embeddingDataType**(): `Float`&lt;`Floats`&gt;

The datatype of the embeddings

#### Returns

`Float`&lt;`Floats`&gt;

#### Overrides

[`EmbeddingFunction`](EmbeddingFunction.md).[`embeddingDataType`](EmbeddingFunction.md#embeddingdatatype)

***

### ndims()

> **ndims**(): `number`

The number of dimensions of the embeddings

#### Returns

`number`

#### Overrides

[`EmbeddingFunction`](EmbeddingFunction.md).[`ndims`](EmbeddingFunction.md#ndims)

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

#### Inherited from

[`EmbeddingFunction`](EmbeddingFunction.md).[`sourceField`](EmbeddingFunction.md#sourcefield)

***

### toJSON()

> **toJSON**(): `object`

Convert the embedding function to a JSON object
It is used to serialize the embedding function to the schema
It's important that any object returned by this method contains all the necessary
information to recreate the embedding function

It should return the same object that was passed to the constructor
If it does not, the embedding function will not be able to be recreated, or could be recreated incorrectly

#### Returns

`object`

##### model

> **model**: `string` & `object` \| `"text-embedding-ada-002"` \| `"text-embedding-3-small"` \| `"text-embedding-3-large"`

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

#### Overrides

[`EmbeddingFunction`](EmbeddingFunction.md).[`toJSON`](EmbeddingFunction.md#tojson)

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

#### Inherited from

[`EmbeddingFunction`](EmbeddingFunction.md).[`vectorField`](EmbeddingFunction.md#vectorfield)
