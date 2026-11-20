[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / OptimizeOptions

# Interface: OptimizeOptions

Flat Optimize settings for submission and durable jobs.
Remote jobs support data rewrites; cleanup options are Embedded-only.

## Properties

### action?

```ts
optional action: "rewrite_data";
```

Run only data rewriting, retaining history and skipping separate index maintenance.

***

### batchSize?

```ts
optional batchSize: number;
```

Positive scanner batch size in rows.

***

### cleanupOlderThan?

```ts
optional cleanupOlderThan: Date;
```

Embedded: remove versions committed before this cutoff. Defaults to seven days ago.
Cannot be combined with action="rewrite_data" or used with durable Optimize jobs.

***

### ~~compactionOptions?~~

```ts
optional compactionOptions: CompactionOptions;
```

#### Deprecated

Pass rewrite settings directly on OptimizeOptions.

***

### deleteUnverified?

```ts
optional deleteUnverified: boolean;
```

Embedded: also remove unverified files. Only enable when no other process
can be writing this dataset. Defaults to false; unavailable for durable jobs.

***

### ioBufferSize?

```ts
optional ioBufferSize: number;
```

Scanner read-ahead budget in bytes; zero disables byte-based backpressure.

***

### maxSourceRows?

```ts
optional maxSourceRows: number;
```

Positive live-source-row budget across whole rewrite tasks.

***

### targetRowsPerFragment?

```ts
optional targetRowsPerFragment: number;
```

Positive target rows per output fragment. Omitted remote values inherit table policy.
