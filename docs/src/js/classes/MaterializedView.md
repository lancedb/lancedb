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

The query that defines the view, read from its stored schema.

#### Returns

`Promise`&lt;[`MaterializedViewDefinition`](../interfaces/MaterializedViewDefinition.md)&gt;

***

### refresh()

```ts
refresh(options?): Promise<RefreshMaterializedViewResult>
```

Recompute the view from its source.

The refresh is incremental when the source's changes can be reconciled
into the view -- rows added, changed or removed since the last one --
and otherwise rebuilds. `full` forces a rebuild; `sourceVersion`
refreshes to that source version instead of the latest.

Concurrent refreshes of one view do not duplicate its rows. Two that
plan the same source rows conflict on commit, and the loser throws
rather than writing them a second time.

#### Parameters

* **options?**

* **options.full?**: `boolean`

* **options.sourceVersion?**: `number`

#### Returns

`Promise`&lt;[`RefreshMaterializedViewResult`](../interfaces/RefreshMaterializedViewResult.md)&gt;

***

### table()

```ts
table(): Table
```

The view, as the table it is.

#### Returns

[`Table`](Table.md)
