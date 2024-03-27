[@lancedb/lancedb](../README.md) / [Exports](../modules.md) / RecordBatchIterator

# Class: RecordBatchIterator

## Implements

- `AsyncIterator`\<`RecordBatch`\>

## Table of contents

### Constructors

- [constructor](RecordBatchIterator.md#constructor)

### Properties

- [inner](RecordBatchIterator.md#inner)
- [promisedInner](RecordBatchIterator.md#promisedinner)

### Methods

- [next](RecordBatchIterator.md#next)

## Constructors

### constructor

• **new RecordBatchIterator**(`promise?`): [`RecordBatchIterator`](RecordBatchIterator.md)

#### Parameters

| Name | Type |
| :------ | :------ |
| `promise?` | `Promise`\<`RecordBatchIterator`\> |

#### Returns

[`RecordBatchIterator`](RecordBatchIterator.md)

#### Defined in

[query.ts:27](https://github.com/lancedb/lancedb/blob/9d178c7/nodejs/lancedb/query.ts#L27)

## Properties

### inner

• `Private` `Optional` **inner**: `RecordBatchIterator`

#### Defined in

[query.ts:25](https://github.com/lancedb/lancedb/blob/9d178c7/nodejs/lancedb/query.ts#L25)

___

### promisedInner

• `Private` `Optional` **promisedInner**: `Promise`\<`RecordBatchIterator`\>

#### Defined in

[query.ts:24](https://github.com/lancedb/lancedb/blob/9d178c7/nodejs/lancedb/query.ts#L24)

## Methods

### next

▸ **next**(): `Promise`\<`IteratorResult`\<`RecordBatch`\<`any`\>, `any`\>\>

#### Returns

`Promise`\<`IteratorResult`\<`RecordBatch`\<`any`\>, `any`\>\>

#### Implementation of

AsyncIterator.next

#### Defined in

[query.ts:33](https://github.com/lancedb/lancedb/blob/9d178c7/nodejs/lancedb/query.ts#L33)
