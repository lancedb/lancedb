[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / JobProgress

# Interface: JobProgress

A forward-compatible structured progress snapshot for a server-side job.

Fields are optional because older servers and different job types report
different counters. A missing counter is unknown; it is not zero.

## Indexable

 \[`field`: `string`\]: `unknown`

## Properties

### estimated\_remaining\_time\_millis?

```ts
optional estimated_remaining_time_millis: number;
```

Best-effort remaining milliseconds, present only for a reliable sample.

***

### execution\_epoch?

```ts
optional execution_epoch: null | number;
```

Execution identity and sample time used for client-side ETA derivation.

***

### paused?

```ts
optional paused: boolean;
```

Whether an operator has parked the job.

***

### phase?

```ts
optional phase: null | string;
```

Coarse server-derived phase, including phases from newer servers.

***

### rows\_computed?

```ts
optional rows_computed: null | number;
```

***

### rows\_failed?

```ts
optional rows_failed: null | number;
```

***

### rows\_planned?

```ts
optional rows_planned: null | number;
```

Planned, computed, skipped, remaining, and failed row counters, when the
server reports them.

***

### rows\_remaining?

```ts
optional rows_remaining: null | number;
```

***

### rows\_skipped?

```ts
optional rows_skipped: null | number;
```

***

### sample\_at\_millis?

```ts
optional sample_at_millis: null | number;
```

***

### state?

```ts
optional state: string;
```

Lifecycle state, including states from newer servers.
