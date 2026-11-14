[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / FunctionRefreshPodResources

# Interface: FunctionRefreshPodResources

CPU, memory, and ephemeral-storage limits for one Function refresh
control-plane pod.

## Properties

### cpuMillis

```ts
cpuMillis: number;
```

CPU limit in millicores; 1000 is one CPU.

***

### ephemeralStorageBytes

```ts
ephemeralStorageBytes: number;
```

Ephemeral storage limit in bytes.

***

### memoryBytes

```ts
memoryBytes: number;
```

Memory limit in bytes.
