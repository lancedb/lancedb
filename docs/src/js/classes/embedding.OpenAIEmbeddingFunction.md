[@lancedb/lancedb](../README.md) / [Exports](../modules.md) / [embedding](../modules/embedding.md) / OpenAIEmbeddingFunction

# Class: OpenAIEmbeddingFunction

[embedding](../modules/embedding.md).OpenAIEmbeddingFunction

An embedding function that automatically creates vector representation for a given column.

## Hierarchy

- [`EmbeddingFunction`](embedding.EmbeddingFunction.md)\<`string`, `Partial`\<[`OpenAIOptions`](../modules/embedding.md#openaioptions)\>\>

  ↳ **`OpenAIEmbeddingFunction`**

## Table of contents

### Constructors

- [constructor](embedding.OpenAIEmbeddingFunction.md#constructor)

### Properties

- [#modelName](embedding.OpenAIEmbeddingFunction.md##modelname)
- [#openai](embedding.OpenAIEmbeddingFunction.md##openai)

### Methods

- [computeQueryEmbeddings](embedding.OpenAIEmbeddingFunction.md#computequeryembeddings)
- [computeSourceEmbeddings](embedding.OpenAIEmbeddingFunction.md#computesourceembeddings)
- [embeddingDataType](embedding.OpenAIEmbeddingFunction.md#embeddingdatatype)
- [ndims](embedding.OpenAIEmbeddingFunction.md#ndims)
- [sourceField](embedding.OpenAIEmbeddingFunction.md#sourcefield)
- [toJSON](embedding.OpenAIEmbeddingFunction.md#tojson)
- [vectorField](embedding.OpenAIEmbeddingFunction.md#vectorfield)

## Constructors

### constructor

• **new OpenAIEmbeddingFunction**(`options?`): [`OpenAIEmbeddingFunction`](embedding.OpenAIEmbeddingFunction.md)

#### Parameters

| Name | Type |
| :------ | :------ |
| `options` | `Partial`\<[`OpenAIOptions`](../modules/embedding.md#openaioptions)\> |

#### Returns

[`OpenAIEmbeddingFunction`](embedding.OpenAIEmbeddingFunction.md)

#### Overrides

[EmbeddingFunction](embedding.EmbeddingFunction.md).[constructor](embedding.EmbeddingFunction.md#constructor)

#### Defined in

[embedding/openai.ts:34](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/embedding/openai.ts#L34)

## Properties

### #modelName

• `Private` **#modelName**: `string` & {} \| ``"text-embedding-ada-002"`` \| ``"text-embedding-3-small"`` \| ``"text-embedding-3-large"``

#### Defined in

[embedding/openai.ts:32](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/embedding/openai.ts#L32)

___

### #openai

• `Private` **#openai**: `OpenAI`

#### Defined in

[embedding/openai.ts:31](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/embedding/openai.ts#L31)

## Methods

### computeQueryEmbeddings

▸ **computeQueryEmbeddings**(`data`): `Promise`\<`number`[]\>

Compute the embeddings for a single query

#### Parameters

| Name | Type |
| :------ | :------ |
| `data` | `string` |

#### Returns

`Promise`\<`number`[]\>

#### Overrides

[EmbeddingFunction](embedding.EmbeddingFunction.md).[computeQueryEmbeddings](embedding.EmbeddingFunction.md#computequeryembeddings)

#### Defined in

[embedding/openai.ts:102](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/embedding/openai.ts#L102)

___

### computeSourceEmbeddings

▸ **computeSourceEmbeddings**(`data`): `Promise`\<`number`[][]\>

Creates a vector representation for the given values.

#### Parameters

| Name | Type |
| :------ | :------ |
| `data` | `string`[] |

#### Returns

`Promise`\<`number`[][]\>

#### Overrides

[EmbeddingFunction](embedding.EmbeddingFunction.md).[computeSourceEmbeddings](embedding.EmbeddingFunction.md#computesourceembeddings)

#### Defined in

[embedding/openai.ts:89](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/embedding/openai.ts#L89)

___

### embeddingDataType

▸ **embeddingDataType**(): `Float`\<`Floats`\>

The datatype of the embeddings

#### Returns

`Float`\<`Floats`\>

#### Overrides

[EmbeddingFunction](embedding.EmbeddingFunction.md).[embeddingDataType](embedding.EmbeddingFunction.md#embeddingdatatype)

#### Defined in

[embedding/openai.ts:85](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/embedding/openai.ts#L85)

___

### ndims

▸ **ndims**(): `number`

The number of dimensions of the embeddings

#### Returns

`number`

#### Overrides

[EmbeddingFunction](embedding.EmbeddingFunction.md).[ndims](embedding.EmbeddingFunction.md#ndims)

#### Defined in

[embedding/openai.ts:72](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/embedding/openai.ts#L72)

___

### sourceField

▸ **sourceField**(`optionsOrDatatype`): [`DataType`\<`Type`, `any`\>, `Map`\<`string`, [`EmbeddingFunction`](embedding.EmbeddingFunction.md)\<`any`, `FunctionOptions`\>\>]

sourceField is used in combination with `LanceSchema` to provide a declarative data model

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `optionsOrDatatype` | `DataType`\<`Type`, `any`\> \| `Partial`\<`FieldOptions`\<`DataType`\<`Type`, `any`\>\>\> | The options for the field or the datatype |

#### Returns

[`DataType`\<`Type`, `any`\>, `Map`\<`string`, [`EmbeddingFunction`](embedding.EmbeddingFunction.md)\<`any`, `FunctionOptions`\>\>]

**`See`**

lancedb.LanceSchema

#### Inherited from

[EmbeddingFunction](embedding.EmbeddingFunction.md).[sourceField](embedding.EmbeddingFunction.md#sourcefield)

#### Defined in

[embedding/embedding_function.ts:91](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/embedding/embedding_function.ts#L91)

___

### toJSON

▸ **toJSON**(): `Object`

Convert the embedding function to a JSON object
It is used to serialize the embedding function to the schema
It's important that any object returned by this method contains all the necessary
information to recreate the embedding function

It should return the same object that was passed to the constructor
If it does not, the embedding function will not be able to be recreated, or could be recreated incorrectly

#### Returns

`Object`

| Name | Type |
| :------ | :------ |
| `model` | `string` & {} \| ``"text-embedding-ada-002"`` \| ``"text-embedding-3-small"`` \| ``"text-embedding-3-large"`` |

**`Example`**

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

[EmbeddingFunction](embedding.EmbeddingFunction.md).[toJSON](embedding.EmbeddingFunction.md#tojson)

#### Defined in

[embedding/openai.ts:66](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/embedding/openai.ts#L66)

___

### vectorField

▸ **vectorField**(`optionsOrDatatype?`): [`DataType`\<`Type`, `any`\>, `Map`\<`string`, [`EmbeddingFunction`](embedding.EmbeddingFunction.md)\<`any`, `FunctionOptions`\>\>]

vectorField is used in combination with `LanceSchema` to provide a declarative data model

#### Parameters

| Name | Type |
| :------ | :------ |
| `optionsOrDatatype?` | `DataType`\<`Type`, `any`\> \| `Partial`\<`FieldOptions`\<`DataType`\<`Type`, `any`\>\>\> |

#### Returns

[`DataType`\<`Type`, `any`\>, `Map`\<`string`, [`EmbeddingFunction`](embedding.EmbeddingFunction.md)\<`any`, `FunctionOptions`\>\>]

**`See`**

lancedb.LanceSchema

#### Inherited from

[EmbeddingFunction](embedding.EmbeddingFunction.md).[vectorField](embedding.EmbeddingFunction.md#vectorfield)

#### Defined in

[embedding/embedding_function.ts:114](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/embedding/embedding_function.ts#L114)
