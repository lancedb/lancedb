[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / CompactionOptions

# Interface: ~~CompactionOptions~~

## Deprecated

Use OptimizeOptions with flat rewrite settings.

## Extends

- `Pick`&lt;[`OptimizeOptions`](OptimizeOptions.md), `"targetRowsPerFragment"` \| `"maxSourceRows"` \| `"ioBufferSize"` \| `"batchSize"`&gt;

## Properties

### ~~batchSize?~~

```ts
optional batchSize: number;
```

Positive scanner batch size in rows.

#### Inherited from

`Pick.batchSize`

***

### ~~ioBufferSize?~~

```ts
optional ioBufferSize: number;
```

Scanner read-ahead budget in bytes; zero disables byte-based backpressure.

#### Inherited from

`Pick.ioBufferSize`

***

### ~~maxSourceRows?~~

```ts
optional maxSourceRows: number;
```

Positive live-source-row budget across whole rewrite tasks.

#### Inherited from

`Pick.maxSourceRows`

***

### ~~targetRowsPerFragment?~~

```ts
optional targetRowsPerFragment: number;
```

Positive target rows per output fragment. Omitted Enterprise values inherit table policy.

#### Inherited from

`Pick.targetRowsPerFragment`
