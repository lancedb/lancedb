[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / UpgradeColumnOptions

# Interface: UpgradeColumnOptions

Options for assigning a new immutable Function version to a column.

## Extends

- `Omit`&lt;[`RefreshColumnOptions`](RefreshColumnOptions.md), `"where"`&gt;

## Properties

### batchSize?

```ts
optional batchSize: number;
```

Maximum rows per Function call; must be greater than zero.

#### Inherited from

`Omit.batchSize`

***

### errorPolicy?

```ts
optional errorPolicy: string;
```

`"fail"` stops on a row error; `"skip"` records it and continues,
subject to the failure limits. The default is server-dependent.

#### Inherited from

`Omit.errorPolicy`

***

### gpuClass?

```ts
optional gpuClass: string;
```

GPU class for Function workers.

#### Inherited from

`Omit.gpuClass`

***

### gpuCount?

```ts
optional gpuCount: number;
```

Positive GPU count allocated to each Function worker.

#### Inherited from

`Omit.gpuCount`

***

### initTimeoutSeconds?

```ts
optional initTimeoutSeconds: number;
```

Deadline for Function initialization, in seconds.

#### Inherited from

`Omit.initTimeoutSeconds`

***

### maxConcurrency?

```ts
optional maxConcurrency: number;
```

Override the registered Function's maximum concurrent user-code calls
for this logical execution.

#### Inherited from

`Omit.maxConcurrency`

***

### maxExecutorProcessesPerWorker?

```ts
optional maxExecutorProcessesPerWorker: number;
```

Cap on executor processes per Function worker.

#### Inherited from

`Omit.maxExecutorProcessesPerWorker`

***

### maxFailedFraction?

```ts
optional maxFailedFraction: number;
```

Under a skip policy, fail the job once more than this share of rows
(0 to 1) has failed.

#### Inherited from

`Omit.maxFailedFraction`

***

### maxFailedRows?

```ts
optional maxFailedRows: number;
```

Under a skip policy, fail the job once more than this many rows have
failed.

#### Inherited from

`Omit.maxFailedRows`

***

### numWorkers?

```ts
optional numWorkers: number;
```

Worker count for an expression column; Function columns reject it.

#### Inherited from

`Omit.numWorkers`

***

### planningWorkers?

```ts
optional planningWorkers: FunctionRefreshPlanningConfig;
```

Resources and sizing for the refresh planning fleet.

#### Inherited from

`Omit.planningWorkers`

***

### publicationIntervalRows?

```ts
optional publicationIntervalRows: number;
```

Row-count threshold for publishing completed Function split ranges.
Must be a positive safe integer. Omit for whole-fragment publication.
Actual publications can be larger; remaining rows publish at completion.
Requires LanceDB Enterprise with non-Blob Function outputs.

#### Inherited from

`Omit.publicationIntervalRows`

***

### recompute?

```ts
optional recompute: FunctionRecompute;
```

Defaults to `"all"`. Quarantined failures are excluded.

***

### retryPolicy?

```ts
optional retryPolicy: FunctionRetryPolicy;
```

Override retries of failed user-code calls for this refresh.

#### Inherited from

`Omit.retryPolicy`

***

### rowTimeoutSeconds?

```ts
optional rowTimeoutSeconds: number;
```

Deadline for one Function call, in seconds.

#### Inherited from

`Omit.rowTimeoutSeconds`

***

### supervisorResources?

```ts
optional supervisorResources: FunctionRefreshPodResources;
```

CPU, memory, and ephemeral-storage limits for the refresh supervisor.

#### Inherited from

`Omit.supervisorResources`

***

### totalCpuMillis?

```ts
optional totalCpuMillis: number;
```

Override the total CPU budget in millicores for this refresh.

#### Inherited from

`Omit.totalCpuMillis`

***

### totalGpuCount?

```ts
optional totalGpuCount: number;
```

Nonnegative total GPU budget for Function workers.

#### Inherited from

`Omit.totalGpuCount`

***

### totalMemoryBytes?

```ts
optional totalMemoryBytes: number;
```

Override the total memory budget in bytes for this refresh.

#### Inherited from

`Omit.totalMemoryBytes`

***

### version

```ts
version: string;
```

An existing immutable version of the bound Function.
