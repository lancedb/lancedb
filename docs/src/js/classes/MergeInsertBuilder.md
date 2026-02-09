[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / MergeInsertBuilder

# Class: MergeInsertBuilder

A builder used to create and run a merge insert operation

## Constructors

### new MergeInsertBuilder()

```ts
new MergeInsertBuilder(native, schema): MergeInsertBuilder
```

Construct a MergeInsertBuilder. __Internal use only.__

#### Parameters

* **native**: `NativeMergeInsertBuilder`

* **schema**: `Schema`&lt;`any`&gt; \| `Promise`&lt;`Schema`&lt;`any`&gt;&gt;

#### Returns

[`MergeInsertBuilder`](MergeInsertBuilder.md)

## Methods

### execute()

```ts
execute(data, execOptions?): Promise<MergeResult>
```

Executes the merge insert operation

#### Parameters

* **data**: [`Data`](../type-aliases/Data.md)

* **execOptions?**: `Partial`&lt;[`WriteExecutionOptions`](../interfaces/WriteExecutionOptions.md)&gt;

#### Returns

`Promise`&lt;[`MergeResult`](../interfaces/MergeResult.md)&gt;

the merge result

***

### useIndex()

```ts
useIndex(useIndex): MergeInsertBuilder
```

Controls whether to use indexes for the merge operation.

When set to `true` (the default), the operation will use an index if available
on the join key for improved performance. When set to `false`, it forces a full
table scan even if an index exists. This can be useful for benchmarking or when
the query optimizer chooses a suboptimal path.

#### Parameters

* **useIndex**: `boolean`
    Whether to use indices for the merge operation. Defaults to `true`.

#### Returns

[`MergeInsertBuilder`](MergeInsertBuilder.md)

***

### whenMatchedUpdateAll()

```ts
whenMatchedUpdateAll(options?): MergeInsertBuilder
```

Rows that exist in both the source table (new data) and
the target table (old data) will be updated, replacing
the old row with the corresponding matching row.

If there are multiple matches then the behavior is undefined.
Currently this causes multiple copies of the row to be created
but that behavior is subject to change.

An optional condition may be specified.  If it is, then only
matched rows that satisfy the condtion will be updated.  Any
rows that do not satisfy the condition will be left as they
are.  Failing to satisfy the condition does not cause a
"matched row" to become a "not matched" row.

The condition should be an SQL string.  Use the prefix
target. to refer to rows in the target table (old data)
and the prefix source. to refer to rows in the source
table (new data).

For example, "target.last_update < source.last_update"

#### Parameters

* **options?**

* **options.where?**: `string`

#### Returns

[`MergeInsertBuilder`](MergeInsertBuilder.md)

***

### whenNotMatchedBySourceDelete()

```ts
whenNotMatchedBySourceDelete(options?): MergeInsertBuilder
```

Rows that exist only in the target table (old data) will be
deleted.  An optional condition can be provided to limit what
data is deleted.

#### Parameters

* **options?**

* **options.where?**: `string`
    An optional condition to limit what data is deleted

#### Returns

[`MergeInsertBuilder`](MergeInsertBuilder.md)

***

### whenNotMatchedInsertAll()

```ts
whenNotMatchedInsertAll(): MergeInsertBuilder
```

Rows that exist only in the source table (new data) should
be inserted into the target table.

#### Returns

[`MergeInsertBuilder`](MergeInsertBuilder.md)
