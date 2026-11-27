[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / TableDelta

# Class: TableDelta

A handle for reading changes between two pinned table versions.

Created by [Table.delta](Table.md#delta). Endpoints are fixed at creation; later writes
or tag moves do not change this delta.

## Accessors

### beginVersion

```ts
get beginVersion(): bigint
```

The exclusive start version, as an exact u64 bigint.

#### Returns

`bigint`

***

### endVersion

```ts
get endVersion(): bigint
```

The inclusive end version, as an exact u64 bigint.

#### Returns

`bigint`

## Methods

### deletedRowIds()

```ts
deletedRowIds(): TableDeltaReader
```

Stream deleted row IDs. Each batch has one `_rowid: uint64` column.

#### Returns

[`TableDeltaReader`](../interfaces/TableDeltaReader.md)

***

### insertedRows()

```ts
insertedRows(): TableDeltaReader
```

Stream rows inserted in `(beginVersion, endVersion]`.

#### Returns

[`TableDeltaReader`](../interfaces/TableDeltaReader.md)

***

### listTransactions()

```ts
listTransactions(): Promise<TableDeltaTransaction[]>
```

List readable transactions committed in `(beginVersion, endVersion]`.

#### Returns

`Promise`&lt;[`TableDeltaTransaction`](../interfaces/TableDeltaTransaction.md)[]&gt;

***

### updatedRows()

```ts
updatedRows(): TableDeltaReader
```

Stream rows updated in `(beginVersion, endVersion]`.

#### Returns

[`TableDeltaReader`](../interfaces/TableDeltaReader.md)

***

### upsertedRows()

```ts
upsertedRows(): TableDeltaReader
```

Stream inserted and updated rows in `(beginVersion, endVersion]`.

#### Returns

[`TableDeltaReader`](../interfaces/TableDeltaReader.md)
