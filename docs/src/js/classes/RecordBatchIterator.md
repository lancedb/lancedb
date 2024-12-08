[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / RecordBatchIterator

# Class: RecordBatchIterator

## Implements

- `AsyncIterator`&lt;`RecordBatch`&gt;

## Constructors

### new RecordBatchIterator()

```ts
new RecordBatchIterator(promise?: Promise<RecordBatchIterator>): RecordBatchIterator
```

#### Parameters

• **promise?**: `Promise`&lt;`RecordBatchIterator`&gt;

#### Returns

[`RecordBatchIterator`](RecordBatchIterator.md)

## Methods

### next()

```ts
next(): Promise<IteratorResult<RecordBatch<any>, any>>
```

#### Returns

`Promise`&lt;`IteratorResult`&lt;`RecordBatch`&lt;`any`&gt;, `any`&gt;&gt;

#### Implementation of

`AsyncIterator.next`
