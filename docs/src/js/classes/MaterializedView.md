[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / MaterializedView

# Class: MaterializedView

A handle on a materialized view: its table plus its definition.

Obtained from [Connection#createMaterializedView](Connection.md#creatematerializedview) or
[Connection#openMaterializedView](Connection.md#openmaterializedview). The view is a normal table --
queries, indexes and search all apply through [MaterializedView#table](MaterializedView.md#table)
-- whose contents are maintained by [MaterializedView#refresh](MaterializedView.md#refresh).

## Constructors

### new MaterializedView()

```ts
new MaterializedView(table): MaterializedView
```

#### Parameters

* **table**: [`Table`](Table.md)

#### Returns

[`MaterializedView`](MaterializedView.md)

## Accessors

### name

```ts
get name(): string
```

#### Returns

`string`

## Methods

### definition()

```ts
definition(): Promise<MaterializedViewDefinition>
```

The query that defines the view.

#### Returns

`Promise`&lt;[`MaterializedViewDefinition`](../interfaces/MaterializedViewDefinition.md)&gt;

***

### refresh()

```ts
refresh(options?): Promise<RefreshMaterializedViewResult>
```

Recompute the view from its source.
Local refresh requires a source build with `experimental-functions`.

`full: true` forces a remote rebuild; otherwise the server chooses the
refresh mode. Local views always rebuild, and `sourceVersion` selects a
source version instead of the latest. Remote SQL refreshes do not support
source-version pinning. Their results leave `mode`, `rowsWritten`, and
`sourceVersion` absent because the SQL job adapter does not expose these metrics.

Resource budgets and GPU placement require a supporting Enterprise server
with an isolated execution backend. Embedded refreshes reject these options.
Budgets apply to the entire execution, not individual Functions or workers,
and do not increase refresh parallelism. Omit them to preserve defaults.

#### Parameters

* **options?**: [`RefreshMaterializedViewOptions`](../interfaces/RefreshMaterializedViewOptions.md)

#### Returns

`Promise`&lt;[`RefreshMaterializedViewResult`](../interfaces/RefreshMaterializedViewResult.md)&gt;

#### Example

```ts
await view.refresh({
  cpuMillis: 8000,
  memoryBytes: 16 * 1024 ** 3,
  gpuCount: 2,
  gpuClass: "h100",
});
```

***

### table()

```ts
table(): Table
```

The view, as the table it is.

#### Returns

[`Table`](Table.md)
