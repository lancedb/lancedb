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
   opts): Promise<Scannable>
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

`Promise`&lt;[`Scannable`](Scannable.md)&gt;

***

### fromIterable()

```ts
static fromIterable(
   schema,
   iter,
   opts): Promise<Scannable>
```

Build a Scannable from an iterable of `RecordBatch`es. `rescannable`
defaults to `false`. Pass an explicit schema so the consumer can
validate before any batch is pulled.

`opts.rescannable: true` is honest for replayable iterables (Arrays,
Sets, or custom iterables whose `[Symbol.iterator]()` returns a fresh
iterator each call). It is rejected for one-shot iterables (generators,
async generators, or already-an-iterator inputs) because their
`[Symbol.iterator]()` returns the same exhausted object on the second
scan. For replayable sources outside this shape, use
`fromFactory(schema, () => createIter(), { rescannable: true })`.

Note: when `opts.rescannable` is `true`, the constructor calls
`[Symbol.iterator]()` once on the input to perform the structural check.

#### Parameters

* **schema**: `Schema`&lt;`any`&gt;

* **iter**: `Iterable`&lt;`RecordBatch`&lt;`any`&gt;&gt; \| `AsyncIterable`&lt;`RecordBatch`&lt;`any`&gt;&gt;

* **opts**: [`ScannableOptions`](../interfaces/ScannableOptions.md) = `{}`

#### Returns

`Promise`&lt;[`Scannable`](Scannable.md)&gt;

***

### fromRecordBatchReader()

```ts
static fromRecordBatchReader(reader, opts): Promise<Scannable>
```

Build a Scannable from an Arrow `RecordBatchReader`. A reader can only
be consumed once; `rescannable` defaults to `false`.

The reader must already be opened (via `.open()`) so its `.schema` is
populated. `RecordBatchReader.from(...)` returns an unopened reader.

`opts.rescannable: true` is rejected because `RecordBatchReader` is a
self-iterator (its `[Symbol.iterator]()` returns itself), and this
constructor does not call `reader.reset()` between scans, so a second
scan would always see an exhausted reader. For genuinely replayable
sources, use
`fromFactory(schema, () => openReader(), { rescannable: true })`,
which mints a fresh reader on each scan.

#### Parameters

* **reader**: `RecordBatchReader`&lt;`any`&gt;

* **opts**: [`ScannableOptions`](../interfaces/ScannableOptions.md) = `{}`

#### Returns

`Promise`&lt;[`Scannable`](Scannable.md)&gt;

***

### fromTable()

```ts
static fromTable(table, opts): Promise<Scannable>
```

Build a Scannable from an in-memory Arrow `Table`. Always rescannable;
the table's batches are replayed on each scan.

The table's row count is authoritative: `opts.numRows` must either be
omitted or equal to `table.numRows`. `opts.rescannable` of `false` is
rejected because in-memory Tables are always rescannable.

#### Parameters

* **table**: `Table`&lt;`any`&gt;

* **opts**: [`ScannableOptions`](../interfaces/ScannableOptions.md) = `{}`

#### Returns

`Promise`&lt;[`Scannable`](Scannable.md)&gt;
