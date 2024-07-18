[@lancedb/lancedb](../README.md) / [Exports](../modules.md) / IndexStatistics

# Interface: IndexStatistics

## Table of contents

### Properties

- [indexType](IndexStatistics.md#indextype)
- [indices](IndexStatistics.md#indices)
- [numIndexedRows](IndexStatistics.md#numindexedrows)
- [numUnindexedRows](IndexStatistics.md#numunindexedrows)

## Properties

### indexType

• `Optional` **indexType**: `string`

The type of the index

#### Defined in

native.d.ts:83

___

### indices

• **indices**: [`IndexMetadata`](IndexMetadata.md)[]

The metadata for each index

#### Defined in

native.d.ts:85

___

### numIndexedRows

• **numIndexedRows**: `number`

The number of rows indexed by the index

#### Defined in

native.d.ts:79

___

### numUnindexedRows

• **numUnindexedRows**: `number`

The number of rows not indexed

#### Defined in

native.d.ts:81
