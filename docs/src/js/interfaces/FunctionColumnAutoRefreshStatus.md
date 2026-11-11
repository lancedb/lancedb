[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / FunctionColumnAutoRefreshStatus

# Interface: FunctionColumnAutoRefreshStatus

Automatic-refresh projection for a Function output.

## Properties

### activeJobId?

```ts
optional activeJobId: string;
```

***

### blockedReason?

```ts
optional blockedReason:
  | "table_has_mem_wal"
  | "inputs_not_settled"
  | "dependency_in_flight"
  | "table_capacity"
  | "cancelled"
  | "preparation_backoff";
```

***

### effectiveCooldownMillis?

```ts
optional effectiveCooldownMillis: bigint;
```

***

### enabled

```ts
enabled: boolean;
```

***

### failureStreak

```ts
failureStreak: number;
```

***

### intervalSeconds?

```ts
optional intervalSeconds: bigint;
```

***

### nextRunAtMillis

```ts
nextRunAtMillis: null | bigint;
```

***

### nextRunAtReason?

```ts
optional nextRunAtReason: string;
```

***

### state

```ts
state:
  | "running"
  | "disabled"
  | "ready"
  | "up_to_date"
  | "waiting_interval"
  | "waiting_backoff"
  | "blocked"
  | "unobservable";
```
