[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / Scannable

# Class: Scannable

A data source that can be scanned as a stream of Arrow `RecordBatch`es.

`Scannable` wraps the schema + optional row count + rescannable flag and
a callback that yields batches one at a time. It is passed to consumers
(e.g. `Table.add`, `createTable`, `mergeInsert` — follow-up work) that
need to pull data without materializing the full dataset in JS memory.

Batches cross the JS↔Rust boundary as Arrow IPC Stream messages; a fresh
writer serializes each batch, and the Rust side decodes it with
`arrow_ipc::reader::StreamReader`. One batch is in flight at a time.

## Properties

### numRows

```ts
readonly numRows: null | number;
```

***

### rescannable

```ts
readonly rescannable: boolean;
```

***

### schema

```ts
readonly schema: Schema<any>;
```

## Methods

### fromFactory()

```ts
static fromFactory(
   schema,
   factory,
   opts): Scannable
```

Build a Scannable from an explicit schema and a factory that returns a
fresh batch iterator on each call.

The factory is invoked once per scan. Each iterator yields
`RecordBatch`es matching the declared schema. Use this when you need
direct control over the pull loop — for example, to wrap a streaming
source whose batches are produced lazily.

#### Parameters

* **schema**: `Schema`&lt;`any`&gt;
    The Arrow schema of the produced batches.

* **factory**
    Called at the start of each scan to produce a batch
    iterator. Must be idempotent when `rescannable` is true.

* **opts**: [`ScannableOptions`](../interfaces/ScannableOptions.md) = `{}`
    Optional hints. `rescannable` defaults to `true`; set to
    `false` if calling `factory()` twice would not reproduce the same data.

#### Returns

[`Scannable`](Scannable.md)

***

### fromIterable()

```ts
static fromIterable(
   schema,
   iter,
   opts): Scannable
```

Build a Scannable from an iterable of `RecordBatch`es. The iterable is
consumed once; `rescannable` defaults to `false`. Pass an explicit
schema so the consumer can validate before any batch is pulled.

#### Parameters

* **schema**: `Schema`&lt;`any`&gt;

* **iter**: `Iterable`&lt;`RecordBatch`&lt;`any`&gt;&gt; \| `AsyncIterable`&lt;`RecordBatch`&lt;`any`&gt;&gt;

* **opts**: [`ScannableOptions`](../interfaces/ScannableOptions.md) = `{}`

#### Returns

[`Scannable`](Scannable.md)

***

### fromRecordBatchReader()

```ts
static fromRecordBatchReader(reader, opts): Scannable
```

Build a Scannable from an Arrow `RecordBatchReader`. A reader can only
be consumed once; `rescannable` defaults to `false`.

The reader must already be opened (via `.open()`) so its `.schema` is
populated — `RecordBatchReader.from(...)` returns an unopened reader.

#### Parameters

* **reader**: `RecordBatchReader`&lt;`any`&gt;

* **opts**: [`ScannableOptions`](../interfaces/ScannableOptions.md) = `{}`

#### Returns

[`Scannable`](Scannable.md)

***

### fromTable()

```ts
static fromTable(table, opts): Scannable
```

Build a Scannable from an in-memory Arrow `Table`. Rescannable by
default — the table's batches are replayed on each scan.

#### Parameters

* **table**: `Table`&lt;`any`&gt;

* **opts**: [`ScannableOptions`](../interfaces/ScannableOptions.md) = `{}`

#### Returns

[`Scannable`](Scannable.md)
