[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / MetricPoint

# Interface: MetricPoint

One aggregated metric data point. For counters and gauges only `value` is
set; for histograms `buckets` (cumulative `le` counts), `count`, and `sum`
are set.

## Properties

### attributes

```ts
attributes: Record<string, string>;
```

***

### buckets?

```ts
optional buckets: MetricBucket[];
```

***

### count?

```ts
optional count: number;
```

***

### kind

```ts
kind: string;
```

***

### name

```ts
name: string;
```

***

### sum?

```ts
optional sum: number;
```

***

### value?

```ts
optional value: number;
```
