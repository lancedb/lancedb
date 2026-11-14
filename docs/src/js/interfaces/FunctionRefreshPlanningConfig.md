[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / FunctionRefreshPlanningConfig

# Interface: FunctionRefreshPlanningConfig

Resources and sizing policy for the planning fleet of one registered
Function refresh.

## Properties

### fragmentsPerWorker

```ts
fragmentsPerWorker: number;
```

Maximum fragments assigned to one planning worker.

***

### maxWorkers

```ts
maxWorkers: number;
```

Maximum number of planning workers.

***

### resources

```ts
resources: FunctionRefreshPodResources;
```

CPU, memory, and ephemeral-storage limits for one planning worker.

***

### rowsPerWorker

```ts
rowsPerWorker: number;
```

Maximum rows assigned to one planning worker.
