[vectordb](../README.md) / [Exports](../saas-modules.md) / RemoteQuery

# Class: Query<T\>

A builder for nearest neighbor queries for LanceDB.

## Type parameters

| Name | Type |
| :------ | :------ |
| `T` | `number`[] |

## Table of contents

### Constructors

- [constructor](RemoteQuery.md#constructor)

### Properties

- [\_embeddings](RemoteQuery.md#_embeddings)
- [\_query](RemoteQuery.md#_query)
- [\_name](RemoteQuery.md#_name)
- [\_client](RemoteQuery.md#_client)

### Methods

- [execute](RemoteQuery.md#execute)


## Constructors

### constructor

• **new Query**<`T`\>(`name`, `client`, `query`, `embeddings?`)

#### Type parameters

| Name | Type |
| :------ | :------ |
| `T` | `number`[] |

#### Parameters

| Name | Type |
| :------ | :------ |
| `name` | `string` |
| `client` | `HttpLancedbClient` |
| `query` | `T` |
| `embeddings?` | [`EmbeddingFunction`](../interfaces/EmbeddingFunction.md)<`T`\> |

#### Defined in

[remote/index.ts:137](https://github.com/lancedb/lancedb/blob/main/node/src/remote/index.ts#L137)

## Methods

### execute

▸ **execute**<`T`\>(): `Promise`<`T`[]\>

Execute the query and return the results as an Array of Objects

#### Type parameters

| Name | Type |
| :------ | :------ |
| `T` | `Record`<`string`, `unknown`\> |

#### Returns

`Promise`<`T`[]\>

#### Defined in

[remote/index.ts:143](https://github.com/lancedb/lancedb/blob/main/node/src/remote/index.ts#L143)