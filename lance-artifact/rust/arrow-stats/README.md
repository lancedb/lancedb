# lance-arrow-stats

Statistics accumulator for [Apache Arrow](https://arrow.apache.org/) arrays.

Computes min, max, null count, NaN count, and buffer memory usage over one or
more batches of Arrow data. Designed for use in Lance's columnar storage layer
where page-level statistics drive predicate pushdown and query planning.

## Usage

```rust
use arrow_array::{Int32Array, ArrayRef};
use lance_arrow_stats::StatisticsAccumulator;
use arrow_schema::DataType;
use std::sync::Arc;

let mut acc = StatisticsAccumulator::new(&DataType::Int32);

let batch: ArrayRef = Arc::new(Int32Array::from(vec![Some(3), None, Some(1), Some(4)]));
acc.update(&batch).unwrap();

let stats = acc.finish();
assert_eq!(stats.null_count, 1);
```

## Tracked Statistics

| Statistic       | Description                                              |
| --------------- | -------------------------------------------------------- |
| `min`           | Minimum non-null, non-NaN value (`ArrowScalar`)          |
| `max`           | Maximum non-null, non-NaN value (`ArrowScalar`)          |
| `null_count`    | Total number of null values                              |
| `nan_count`     | Total NaN values (float and float-list types only)       |
| `item_nulls`    | Null items inside list entries (list types only)          |
| `buffer_memory` | Total Arrow buffer memory in bytes                       |

## Supported Types

- **Numeric** &mdash; Int8–Int64, UInt8–UInt64, Float16/32/64
- **Temporal** &mdash; Date32/64, Time32/64, Timestamp, Duration
- **Boolean**
- **String** &mdash; Utf8, LargeUtf8
- **Binary** &mdash; Binary, LargeBinary
- **List** &mdash; List, LargeList, FixedSizeList (computes stats over items)

Dictionary, run-end encoded, and view types are accepted but min/max will be
`None`.

## Merging

Accumulators of the same data type can be merged, which is useful for combining
statistics computed in parallel across different pages or files:

```rust
use lance_arrow_stats::StatisticsAccumulator;
use arrow_schema::DataType;

let mut a = StatisticsAccumulator::new(&DataType::Float32);
let mut b = StatisticsAccumulator::new(&DataType::Float32);
// ... update each with different batches ...
a.merge(&b).unwrap();
```
