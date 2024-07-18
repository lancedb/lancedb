[@lancedb/lancedb](../README.md) / [Exports](../modules.md) / [embedding](../modules/embedding.md) / EmbeddingFunction

# Class: EmbeddingFunction\<T, M\>

[embedding](../modules/embedding.md).EmbeddingFunction

An embedding function that automatically creates vector representation for a given column.

## Type parameters

| Name | Type |
| :------ | :------ |
| `T` | `any` |
| `M` | extends `FunctionOptions` = `FunctionOptions` |

## Hierarchy

- **`EmbeddingFunction`**

  ↳ [`OpenAIEmbeddingFunction`](embedding.OpenAIEmbeddingFunction.md)

## Table of contents

### Constructors

- [constructor](embedding.EmbeddingFunction.md#constructor)

### Methods

- [computeQueryEmbeddings](embedding.EmbeddingFunction.md#computequeryembeddings)
- [computeSourceEmbeddings](embedding.EmbeddingFunction.md#computesourceembeddings)
- [embeddingDataType](embedding.EmbeddingFunction.md#embeddingdatatype)
- [ndims](embedding.EmbeddingFunction.md#ndims)
- [sourceField](embedding.EmbeddingFunction.md#sourcefield)
- [toJSON](embedding.EmbeddingFunction.md#tojson)
- [vectorField](embedding.EmbeddingFunction.md#vectorfield)

## Constructors

### constructor

• **new EmbeddingFunction**\<`T`, `M`\>(): [`EmbeddingFunction`](embedding.EmbeddingFunction.md)\<`T`, `M`\>

#### Type parameters

| Name | Type |
| :------ | :------ |
| `T` | `any` |
| `M` | extends `FunctionOptions` = `FunctionOptions` |

#### Returns

[`EmbeddingFunction`](embedding.EmbeddingFunction.md)\<`T`, `M`\>

## Methods

### computeQueryEmbeddings

▸ **computeQueryEmbeddings**(`data`): `Promise`\<`number`[] \| `Float32Array` \| `Float64Array`\>

Compute the embeddings for a single query

#### Parameters

| Name | Type |
| :------ | :------ |
| `data` | `T` |

#### Returns

`Promise`\<`number`[] \| `Float32Array` \| `Float64Array`\>

#### Defined in

[embedding/embedding_function.ts:184](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/embedding/embedding_function.ts#L184)

___

### computeSourceEmbeddings

▸ **computeSourceEmbeddings**(`data`): `Promise`\<`number`[][] \| `Float32Array`[] \| `Float64Array`[]\>

Creates a vector representation for the given values.

#### Parameters

| Name | Type |
| :------ | :------ |
| `data` | `T`[] |

#### Returns

`Promise`\<`number`[][] \| `Float32Array`[] \| `Float64Array`[]\>

#### Defined in

[embedding/embedding_function.ts:177](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/embedding/embedding_function.ts#L177)

___

### embeddingDataType

▸ **embeddingDataType**(): `Float`\<`Floats`\>

The datatype of the embeddings

#### Returns

`Float`\<`Floats`\>

#### Defined in

[embedding/embedding_function.ts:172](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/embedding/embedding_function.ts#L172)

___

### ndims

▸ **ndims**(): `undefined` \| `number`

The number of dimensions of the embeddings

#### Returns

`undefined` \| `number`

#### Defined in

[embedding/embedding_function.ts:167](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/embedding/embedding_function.ts#L167)

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

#### Defined in

[embedding/embedding_function.ts:91](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/embedding/embedding_function.ts#L91)

___

### toJSON

▸ **toJSON**(): `Partial`\<`M`\>

Convert the embedding function to a JSON object
It is used to serialize the embedding function to the schema
It's important that any object returned by this method contains all the necessary
information to recreate the embedding function

It should return the same object that was passed to the constructor
If it does not, the embedding function will not be able to be recreated, or could be recreated incorrectly

#### Returns

`Partial`\<`M`\>

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

#### Defined in

[embedding/embedding_function.ts:82](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/embedding/embedding_function.ts#L82)

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

#### Defined in

[embedding/embedding_function.ts:114](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/embedding/embedding_function.ts#L114)
