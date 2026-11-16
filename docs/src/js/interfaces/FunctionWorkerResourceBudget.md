[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / FunctionWorkerResourceBudget

# Interface: FunctionWorkerResourceBudget

Aggregate worker resources for one Enterprise Function refresh.

## Properties

### cpuMillis

```ts
cpuMillis: number;
```

Aggregate CPU budget in millicores; 1000 is one CPU.

***

### gpuCount?

```ts
optional gpuCount: number;
```

Aggregate GPU budget. Omitted means zero, a CPU-only refresh.

***

### memoryBytes

```ts
memoryBytes: number;
```

Aggregate memory budget in bytes.
