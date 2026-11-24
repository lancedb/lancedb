[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / TableDeltaReader

# Interface: TableDeltaReader

A streaming table-delta reader.

`schema()` is available before consuming the stream, including when the
range is empty. The stream yields Arrow record batches without
materializing the full result.

A reader owns one native cursor. It is single-use:
`[Symbol.asyncIterator]()` returns the same iterator, concurrent pulls are
serialized, and `return()` releases the native stream on early drop.

## Extends

- `AsyncIterable`&lt;`RecordBatch`&gt;.`AsyncIterator`&lt;`RecordBatch`&gt;

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

### next()

```ts
next(...args): Promise<IteratorResult<RecordBatch<any>, any>>
```

#### Parameters

* ...**args**: [] \| [`undefined`]

#### Returns

`Promise`&lt;`IteratorResult`&lt;`RecordBatch`&lt;`any`&gt;, `any`&gt;&gt;

#### Inherited from

`AsyncIterator.next`

***

### return()?

```ts
optional return(value?): Promise<IteratorResult<RecordBatch<any>, any>>
```

#### Parameters

* **value?**: `any`

#### Returns

`Promise`&lt;`IteratorResult`&lt;`RecordBatch`&lt;`any`&gt;, `any`&gt;&gt;

#### Inherited from

`AsyncIterator.return`

***

### schema()

```ts
schema(): Promise<Schema<any>>
```

#### Returns

`Promise`&lt;`Schema`&lt;`any`&gt;&gt;

***

### throw()?

```ts
optional throw(e?): Promise<IteratorResult<RecordBatch<any>, any>>
```

#### Parameters

* **e?**: `any`

#### Returns

`Promise`&lt;`IteratorResult`&lt;`RecordBatch`&lt;`any`&gt;, `any`&gt;&gt;

#### Inherited from

`AsyncIterator.throw`
