[vectordb](../README.md) / [Exports](../modules.md) / CompactionOptions

# Interface: CompactionOptions

## Table of contents

### Properties

- [materializeDeletions](CompactionOptions.md#materializedeletions)
- [materializeDeletionsThreshold](CompactionOptions.md#materializedeletionsthreshold)
- [maxRowsPerGroup](CompactionOptions.md#maxrowspergroup)
- [numThreads](CompactionOptions.md#numthreads)
- [targetRowsPerFragment](CompactionOptions.md#targetrowsperfragment)

## Properties

### materializeDeletions

• `Optional` **materializeDeletions**: `boolean`

If true, fragments that have rows that are deleted may be compacted to
remove the deleted rows. This can improve the performance of queries.
Default is true.

#### Defined in

[index.ts:901](https://github.com/lancedb/lancedb/blob/c89d5e6/node/src/index.ts#L901)

___

### materializeDeletionsThreshold

• `Optional` **materializeDeletionsThreshold**: `number`

A number between 0 and 1, representing the proportion of rows that must be
marked deleted before a fragment is a candidate for compaction to remove
the deleted rows. Default is 10%.

#### Defined in

[index.ts:907](https://github.com/lancedb/lancedb/blob/c89d5e6/node/src/index.ts#L907)

___

### maxRowsPerGroup

• `Optional` **maxRowsPerGroup**: `number`

The maximum number of rows per group. Defaults to 1024.

#### Defined in

[index.ts:895](https://github.com/lancedb/lancedb/blob/c89d5e6/node/src/index.ts#L895)

___

### numThreads

• `Optional` **numThreads**: `number`

The number of threads to use for compaction. If not provided, defaults to
the number of cores on the machine.

#### Defined in

[index.ts:912](https://github.com/lancedb/lancedb/blob/c89d5e6/node/src/index.ts#L912)

___

### targetRowsPerFragment

• `Optional` **targetRowsPerFragment**: `number`

The number of rows per fragment to target. Fragments that have fewer rows
will be compacted into adjacent fragments to produce larger fragments.
Defaults to 1024 * 1024.

#### Defined in

[index.ts:891](https://github.com/lancedb/lancedb/blob/c89d5e6/node/src/index.ts#L891)
