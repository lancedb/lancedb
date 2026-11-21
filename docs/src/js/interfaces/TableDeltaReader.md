[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / TableDeltaReader

# Interface: TableDeltaReader

A streaming table-delta reader.

`schema()` is available before consuming the stream, including when the
range is empty. The stream yields Arrow record batches without
materializing the full result.

## Extends

- `AsyncIterable`&lt;`RecordBatch`&gt;

## Methods

### \[asyncIterator\]()

```ts
asyncIterator: AsyncIterator<RecordBatch<any>, any, undefined>
```

#### Returns

`AsyncIterator`&lt;`RecordBatch`&lt;`any`&gt;, `any`, `undefined`&gt;

#### Inherited from

`AsyncIterable.[asyncIterator]`

***

### schema()

```ts
schema(): Promise<Schema<any>>
```

#### Returns

`Promise`&lt;`Schema`&lt;`any`&gt;&gt;
