[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / permutationBuilder

# Function: permutationBuilder()

```ts
function permutationBuilder(table): PermutationBuilder
```

Create a permutation builder for the given table.

## Parameters

* **table**: [`Table`](../classes/Table.md)
    The source table to create a permutation from

## Returns

[`PermutationBuilder`](../classes/PermutationBuilder.md)

A PermutationBuilder instance

## Example

```ts
const builder = permutationBuilder(sourceTable, "training_data")
  .splitRandom({ ratios: [0.8, 0.2], seed: 42 })
  .shuffle({ seed: 123 });

const trainingTable = await builder.execute();
```
