[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / MakeArrowTableOptions

# Class: MakeArrowTableOptions

Options to control the makeArrowTable call.

## Constructors

### new MakeArrowTableOptions()

> **new MakeArrowTableOptions**(`values`?): [`MakeArrowTableOptions`](MakeArrowTableOptions.md)

#### Parameters

• **values?**: `Partial`&lt;[`MakeArrowTableOptions`](MakeArrowTableOptions.md)&gt;

#### Returns

[`MakeArrowTableOptions`](MakeArrowTableOptions.md)

## Properties

### dictionaryEncodeStrings

> **dictionaryEncodeStrings**: `boolean` = `false`

If true then string columns will be encoded with dictionary encoding

Set this to true if your string columns tend to repeat the same values
often.  For more precise control use the `schema` property to specify the
data type for individual columns.

If `schema` is provided then this property is ignored.

***

### embeddingFunction?

> `optional` **embeddingFunction**: [`EmbeddingFunctionConfig`](../namespaces/embedding/interfaces/EmbeddingFunctionConfig.md)

***

### embeddings?

> `optional` **embeddings**: [`EmbeddingFunction`](../namespaces/embedding/classes/EmbeddingFunction.md)&lt;`unknown`, `FunctionOptions`&gt;

***

### schema?

> `optional` **schema**: `SchemaLike`

***

### vectorColumns

> **vectorColumns**: `Record`&lt;`string`, [`VectorColumnOptions`](VectorColumnOptions.md)&gt;
