[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / FunctionColumnStatus

# Interface: FunctionColumnStatus

Exact four-way status of one Function output at one table version.

## Properties

### autoRefresh

```ts
autoRefresh: FunctionColumnAutoRefreshStatus;
```

***

### bindingId

```ts
bindingId: string;
```

***

### column

```ts
column: string;
```

***

### failed

```ts
failed: bigint;
```

***

### fresh

```ts
fresh: bigint;
```

***

### function

```ts
function: FunctionColumnVersionStatus;
```

***

### lastFailureJob?

```ts
optional lastFailureJob: FunctionColumnLastRefreshJob;
```

Newest failed refresh Job. This remains present when `lastRefreshJob`
is a later cancellation.

***

### lastRefreshJob?

```ts
optional lastRefreshJob: FunctionColumnLastRefreshJob;
```

***

### neverComputed

```ts
neverComputed: bigint;
```

***

### rows

```ts
rows: bigint;
```

***

### stale

```ts
stale: bigint;
```

***

### tableVersion

```ts
tableVersion: bigint;
```
