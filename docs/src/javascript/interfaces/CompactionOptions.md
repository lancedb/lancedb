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

[index.ts:1241](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L1241)

___

### materializeDeletionsThreshold

• `Optional` **materializeDeletionsThreshold**: `number`

A number between 0 and 1, representing the proportion of rows that must be
marked deleted before a fragment is a candidate for compaction to remove
the deleted rows. Default is 10%.

#### Defined in

[index.ts:1247](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L1247)

___

### maxRowsPerGroup

• `Optional` **maxRowsPerGroup**: `number`

The maximum number of T per group. Defaults to 1024.

#### Defined in

[index.ts:1235](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L1235)

___

### numThreads

• `Optional` **numThreads**: `number`

The number of threads to use for compaction. If not provided, defaults to
the number of cores on the machine.

#### Defined in

[index.ts:1252](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L1252)

___

### targetRowsPerFragment

• `Optional` **targetRowsPerFragment**: `number`

The number of rows per fragment to target. Fragments that have fewer rows
will be compacted into adjacent fragments to produce larger fragments.
Defaults to 1024 * 1024.

#### Defined in

[index.ts:1231](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L1231)
