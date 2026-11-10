[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / RefreshColumnOptions

# Interface: RefreshColumnOptions

Options for `Table.refreshColumn` and `Table.refreshColumnAsync`. Every
option is LanceDB Enterprise only; a LanceDB Embedded refresh ignores
`batchSize`, `gpuClass` and `gpuCount` and rejects the rest, including
`resourceBudget`.

## Properties

### batchSize?

```ts
optional batchSize: number;
```

Maximum rows per Function call; must be greater than zero.

***

### errorPolicy?

```ts
optional errorPolicy: string;
```

`"fail"` (default) stops on a row error; `"skip"` writes null and
records the error, subject to the failure limits.

***

### gpuClass?

```ts
optional gpuClass: string;
```

GPU class for Function workers.

***

### gpuCount?

```ts
optional gpuCount: number;
```

Positive aggregate GPU budget for Function workers.

***

### initTimeoutSeconds?

```ts
optional initTimeoutSeconds: number;
```

Deadline for Function initialization, in seconds.

***

### maxExecutorProcessesPerWorker?

```ts
optional maxExecutorProcessesPerWorker: number;
```

Cap on executor processes per Function worker.

***

### maxFailedFraction?

```ts
optional maxFailedFraction: number;
```

Under a skip policy, fail the job once more than this share of rows
(0 to 1) has failed.

***

### maxFailedRows?

```ts
optional maxFailedRows: number;
```

Under a skip policy, fail the job once more than this many rows have
failed.

***

### numWorkers?

```ts
optional numWorkers: number;
```

Worker count for an expression column; Function columns reject it.

***

### resourceBudget?

```ts
optional resourceBudget: FunctionWorkerResourceBudget;
```

Complete aggregate CPU and memory budget for a registered Function
refresh. `gpuCount` inside the budget defaults to zero.

***

### rowTimeoutSeconds?

```ts
optional rowTimeoutSeconds: number;
```

Deadline for one Function call, in seconds.

***

### where?

```ts
optional where: string;
```

Only refresh the rows matching this SQL filter.
