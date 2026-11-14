[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / BloomFilterOptions

# Interface: BloomFilterOptions

Options to create a Bloom filter index.

## Properties

### numberOfItems?

```ts
optional numberOfItems: number;
```

The number of rows covered by each Bloom filter. Must be a positive safe integer.

Defaults to 8192, unless overridden by Lance's
`LANCE_BLOOMFILTER_DEFAULT_NUMBER_OF_ITEMS` environment variable.

***

### probability?

```ts
optional probability: number;
```

The desired false-positive probability. Must be finite and strictly between zero and one.
Lower values use more space.

Defaults to 0.00057, unless overridden by Lance's
`LANCE_BLOOMFILTER_DEFAULT_PROBABILITY` environment variable.
