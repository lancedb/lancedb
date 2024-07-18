[@lancedb/lancedb](../README.md) / [Exports](../modules.md) / [embedding](../modules/embedding.md) / EmbeddingFunctionRegistry

# Class: EmbeddingFunctionRegistry

[embedding](../modules/embedding.md).EmbeddingFunctionRegistry

This is a singleton class used to register embedding functions
and fetch them by name. It also handles serializing and deserializing.
You can implement your own embedding function by subclassing EmbeddingFunction
or TextEmbeddingFunction and registering it with the registry

## Table of contents

### Constructors

- [constructor](embedding.EmbeddingFunctionRegistry.md#constructor)

### Properties

- [#functions](embedding.EmbeddingFunctionRegistry.md##functions)

### Methods

- [functionToMetadata](embedding.EmbeddingFunctionRegistry.md#functiontometadata)
- [get](embedding.EmbeddingFunctionRegistry.md#get)
- [getTableMetadata](embedding.EmbeddingFunctionRegistry.md#gettablemetadata)
- [register](embedding.EmbeddingFunctionRegistry.md#register)
- [reset](embedding.EmbeddingFunctionRegistry.md#reset)

## Constructors

### constructor

• **new EmbeddingFunctionRegistry**(): [`EmbeddingFunctionRegistry`](embedding.EmbeddingFunctionRegistry.md)

#### Returns

[`EmbeddingFunctionRegistry`](embedding.EmbeddingFunctionRegistry.md)

## Properties

### #functions

• `Private` **#functions**: `Map`\<`string`, `EmbeddingFunctionConstructor`\<[`EmbeddingFunction`](embedding.EmbeddingFunction.md)\<`any`, `FunctionOptions`\>\>\>

#### Defined in

[embedding/registry.ts:33](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/embedding/registry.ts#L33)

## Methods

### functionToMetadata

▸ **functionToMetadata**(`conf`): `Record`\<`string`, `any`\>

#### Parameters

| Name | Type |
| :------ | :------ |
| `conf` | [`EmbeddingFunctionConfig`](../interfaces/embedding.EmbeddingFunctionConfig.md) |

#### Returns

`Record`\<`string`, `any`\>

#### Defined in

[embedding/registry.ts:143](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/embedding/registry.ts#L143)

___

### get

▸ **get**\<`T`, `Name`\>(`name`): `Name` extends ``"openai"`` ? `EmbeddingFunctionCreate`\<[`OpenAIEmbeddingFunction`](embedding.OpenAIEmbeddingFunction.md)\> : `undefined` \| `EmbeddingFunctionCreate`\<`T`\>

Fetch an embedding function by name

#### Type parameters

| Name | Type |
| :------ | :------ |
| `T` | extends [`EmbeddingFunction`](embedding.EmbeddingFunction.md)\<`unknown`, `FunctionOptions`\> |
| `Name` | extends `string` = ``""`` |

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `name` | `Name` extends ``"openai"`` ? ``"openai"`` : `string` | The name of the function |

#### Returns

`Name` extends ``"openai"`` ? `EmbeddingFunctionCreate`\<[`OpenAIEmbeddingFunction`](embedding.OpenAIEmbeddingFunction.md)\> : `undefined` \| `EmbeddingFunctionCreate`\<`T`\>

#### Defined in

[embedding/registry.ts:68](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/embedding/registry.ts#L68)

___

### getTableMetadata

▸ **getTableMetadata**(`functions`): `Map`\<`string`, `string`\>

#### Parameters

| Name | Type |
| :------ | :------ |
| `functions` | [`EmbeddingFunctionConfig`](../interfaces/embedding.EmbeddingFunctionConfig.md)[] |

#### Returns

`Map`\<`string`, `string`\>

#### Defined in

[embedding/registry.ts:157](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/embedding/registry.ts#L157)

___

### register

▸ **register**\<`T`\>(`this`, `alias?`): (`ctor`: `T`) => `any`

Register an embedding function

#### Type parameters

| Name | Type |
| :------ | :------ |
| `T` | extends `EmbeddingFunctionConstructor`\<[`EmbeddingFunction`](embedding.EmbeddingFunction.md)\<`any`, `FunctionOptions`\>\> = `EmbeddingFunctionConstructor`\<[`EmbeddingFunction`](embedding.EmbeddingFunction.md)\<`any`, `FunctionOptions`\>\> |

#### Parameters

| Name | Type |
| :------ | :------ |
| `this` | [`EmbeddingFunctionRegistry`](embedding.EmbeddingFunctionRegistry.md) |
| `alias?` | `string` |

#### Returns

`fn`

▸ (`ctor`): `any`

##### Parameters

| Name | Type |
| :------ | :------ |
| `ctor` | `T` |

##### Returns

`any`

**`Throws`**

Error if the function is already registered

#### Defined in

[embedding/registry.ts:41](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/embedding/registry.ts#L41)

___

### reset

▸ **reset**(`this`): `void`

reset the registry to the initial state

#### Parameters

| Name | Type |
| :------ | :------ |
| `this` | [`EmbeddingFunctionRegistry`](embedding.EmbeddingFunctionRegistry.md) |

#### Returns

`void`

#### Defined in

[embedding/registry.ts:101](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/embedding/registry.ts#L101)
