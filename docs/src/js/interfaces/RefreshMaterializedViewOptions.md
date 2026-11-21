[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / RefreshMaterializedViewOptions

# Interface: RefreshMaterializedViewOptions

Options for [MaterializedView.refresh](../classes/MaterializedView.md#refresh).

CPU or memory requires all three budget fields, including an explicit
`gpuCount` (zero for CPU-only execution). A GPU-only cap or class-only
placement is also supported. Resource settings require Enterprise support
and are rejected by Embedded refreshes.

## Properties

### cpuMillis?

```ts
optional cpuMillis: number;
```

Aggregate CPU capacity in millicores (1000 = one CPU), as a positive
safe integer. Requires memoryBytes and gpuCount.

***

### full?

```ts
optional full: boolean;
```

Force a remote rebuild. Defaults to false; Embedded always rebuilds.

***

### gpuClass?

```ts
optional gpuClass: string;
```

Operator-configured GPU class for this execution. Use a lowercase
DNS label of at most 63 characters (letters, digits, and interior
hyphens). May be specified without an explicit GPU budget.

***

### gpuCount?

```ts
optional gpuCount: number;
```

Maximum concurrent GPUs across the refresh, from 0 to 4294967295.
Zero requests CPU-only execution and cannot be combined with gpuClass.

***

### memoryBytes?

```ts
optional memoryBytes: number;
```

Aggregate memory cap in bytes, as a positive safe integer.
Requires cpuMillis and gpuCount.

***

### sourceVersion?

```ts
optional sourceVersion: number;
```

Pin the source version for an Embedded refresh. Unsupported remotely.
