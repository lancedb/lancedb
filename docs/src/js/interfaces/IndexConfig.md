[@lancedb/lancedb](../README.md) / [Exports](../modules.md) / IndexConfig

# Interface: IndexConfig

A description of an index currently configured on a column

## Table of contents

### Properties

- [columns](IndexConfig.md#columns)
- [indexType](IndexConfig.md#indextype)

## Properties

### columns

• **columns**: `string`[]

The columns in the index

Currently this is always an array of size 1.  In the future there may
be more columns to represent composite indices.

#### Defined in

native.d.ts:16

___

### indexType

• **indexType**: `string`

The type of the index

#### Defined in

native.d.ts:9
