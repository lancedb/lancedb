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

[query.ts:32](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/query.ts#L32)

## Properties

### inner

• `Private` `Optional` **inner**: `RecordBatchIterator`

#### Defined in

[query.ts:30](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/query.ts#L30)

___

### promisedInner

• `Private` `Optional` **promisedInner**: `Promise`\<`RecordBatchIterator`\>

#### Defined in

[query.ts:29](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/query.ts#L29)

## Methods

### next

▸ **next**(): `Promise`\<`IteratorResult`\<`RecordBatch`\<`any`\>, `any`\>\>

#### Returns

`Promise`\<`IteratorResult`\<`RecordBatch`\<`any`\>, `any`\>\>

#### Implementation of

AsyncIterator.next

#### Defined in

[query.ts:38](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/query.ts#L38)
