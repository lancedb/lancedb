[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / PermutationBuilder

# Class: PermutationBuilder

A PermutationBuilder for creating data permutations with splits, shuffling, and filtering.

This class provides a TypeScript wrapper around the native Rust PermutationBuilder,
offering methods to configure data splits, shuffling, and filtering before executing
the permutation to create a new table.

## Methods

### execute()

```ts
execute(): Promise<Table>
```

Execute the permutation and create the destination table.

#### Returns

`Promise`&lt;[`Table`](Table.md)&gt;

A Promise that resolves to the new Table instance

#### Example

```ts
const permutationTable = await builder.execute();
console.log(`Created table: ${permutationTable.name}`);
```

***

### filter()

```ts
filter(filter): PermutationBuilder
```

Configure filtering for the permutation.

#### Parameters

* **filter**: `string`
    SQL filter expression

#### Returns

[`PermutationBuilder`](PermutationBuilder.md)

A new PermutationBuilder instance

#### Example

```ts
builder.filter("age > 18 AND status = 'active'");
```

***

### shuffle()

```ts
shuffle(options): PermutationBuilder
```

Configure shuffling for the permutation.

#### Parameters

* **options**: [`ShuffleOptions`](../interfaces/ShuffleOptions.md)
    Configuration for shuffling

#### Returns

[`PermutationBuilder`](PermutationBuilder.md)

A new PermutationBuilder instance

#### Example

```ts
// Basic shuffle
builder.shuffle({ seed: 42 });

// Shuffle with clump size
builder.shuffle({ seed: 42, clumpSize: 10 });
```

***

### splitCalculated()

```ts
splitCalculated(calculation): PermutationBuilder
```

Configure calculated splits for the permutation.

#### Parameters

* **calculation**: `string`
    SQL expression for calculating splits

#### Returns

[`PermutationBuilder`](PermutationBuilder.md)

A new PermutationBuilder instance

#### Example

```ts
builder.splitCalculated("user_id % 3");
```

***

### splitHash()

```ts
splitHash(options): PermutationBuilder
```

Configure hash-based splits for the permutation.

#### Parameters

* **options**: [`SplitHashOptions`](../interfaces/SplitHashOptions.md)
    Configuration for hash-based splitting

#### Returns

[`PermutationBuilder`](PermutationBuilder.md)

A new PermutationBuilder instance

#### Example

```ts
builder.splitHash({
  columns: ["user_id"],
  splitWeights: [70, 30],
  discardWeight: 0
});
```

***

### splitRandom()

```ts
splitRandom(options): PermutationBuilder
```

Configure random splits for the permutation.

#### Parameters

* **options**: [`SplitRandomOptions`](../interfaces/SplitRandomOptions.md)
    Configuration for random splitting

#### Returns

[`PermutationBuilder`](PermutationBuilder.md)

A new PermutationBuilder instance

#### Example

```ts
// Split by ratios
builder.splitRandom({ ratios: [0.7, 0.3], seed: 42 });

// Split by counts
builder.splitRandom({ counts: [1000, 500], seed: 42 });

// Split with fixed size
builder.splitRandom({ fixed: 100, seed: 42 });
```

***

### splitSequential()

```ts
splitSequential(options): PermutationBuilder
```

Configure sequential splits for the permutation.

#### Parameters

* **options**: [`SplitSequentialOptions`](../interfaces/SplitSequentialOptions.md)
    Configuration for sequential splitting

#### Returns

[`PermutationBuilder`](PermutationBuilder.md)

A new PermutationBuilder instance

#### Example

```ts
// Split by ratios
builder.splitSequential({ ratios: [0.8, 0.2] });

// Split by counts
builder.splitSequential({ counts: [800, 200] });

// Split with fixed size
builder.splitSequential({ fixed: 1000 });
```
