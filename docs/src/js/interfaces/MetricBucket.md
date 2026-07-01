[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / MetricBucket

# Interface: MetricBucket

One cumulative histogram bucket: all samples with value `<= le`.

## Properties

### cumulativeCount

```ts
cumulativeCount: number;
```

Cumulative number of samples less than or equal to `le`.

***

### le

```ts
le: string;
```

The inclusive upper bound of the bucket, or `"+Inf"` for the final bucket.
