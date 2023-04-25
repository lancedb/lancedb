# ANN (Approximate Nearest Neighbor) Indexes

You can create an index over your vector data to make search faster.
Vector indexes are faster but less accurate than exhaustive search.
LanceDB provides many parameters to fine-tune the index's size, the speed of queries, and the accuracy of results.

Currently, LanceDB does *not* automatically create the ANN index.
LanceDB has optimized code for KNN as well. For many use-cases, datasets under 100K vectors won't require index creation at all.
If you can live with <100ms latency, skipping index creation is a simpler workflow while guaranteeing 100% recall.

In the future we will look to automatically create and configure the ANN index.

## Creating an ANN Index

Creating indexes is done via the [create_index](https://lancedb.github.io/lancedb/python/#lancedb.table.LanceTable.create_index) method.

```python
import lancedb
import numpy as np
uri = "~/.lancedb"
db = lancedb.connect(uri)

# Create 10,000 sample vectors
data = [{"vector": row, "item": f"item {i}"}
   for i, row in enumerate(np.random.random((10_000, 768)).astype('float32'))]

# Add the vectors to a table
tbl = db.create_table("my_vectors", data=data)

# Create and train the index - you need to have enough data in the table for an effective training step
tbl.create_index(num_partitions=256, num_sub_vectors=96)
```

Since `create_index` has a training step, it can take a few minutes to finish for large tables. You can control the index
creation by providing the following parameters:

- **num_partitions** (default: 256): The number of partitions of the index. The number of partitions should be configured so each partition has 3-5K vectors. For example, a table
with ~1M vectors should use 256 partitions. You can specify arbitrary number of partitions but powers of 2 is most conventional.
A higher number leads to faster queries, but it makes index generation slower.
- **num_sub_vectors** (default: 96): The number of subvectors (M) that will be created during Product Quantization (PQ). A larger number makes
search more accurate, but also makes the index larger and slower to build.

## Querying an ANN Index

Querying vector indexes is done via the [search](https://lancedb.github.io/lancedb/python/#lancedb.table.LanceTable.search) function.

There are a couple of parameters that can be used to fine-tune the search:

- **limit** (default: 10): The amount of results that will be returned
- **nprobes** (default: 20): The number of probes used. A higher number makes search more accurate but also slower.<br/>
  Most of the time, setting nprobes to cover 5-10% of the dataset should achieve high recall with low latency.<br/>
  e.g., for 1M vectors divided up into 256 partitions, nprobes should be set to ~20-40.<br/>
  Note: nprobes is only applicable if an ANN index is present. If specified on a table without an ANN index, it is ignored.
- **refine_factor** (default: None): Refine the results by reading extra elements and re-ranking them in memory.<br/>
  A higher number makes search more accurate but also slower. If you find the recall is less than idea, try refine_factor=10 to start.<br/>
  e.g., for 1M vectors divided into 256 partitions, if you're looking for top 20, then refine_factor=200 reranks the whole partition.<br/>
  Note: refine_factor is only applicable if an ANN index is present. If specified on a table without an ANN index, it is ignored.

```python
tbl.search(np.random.random((768))) \
    .limit(2) \
    .nprobes(20) \
    .refine_factor(10) \
    .to_df()

                                              vector       item       score
0  [0.44949695, 0.8444449, 0.06281311, 0.23338133...  item 1141  103.575333
1  [0.48587373, 0.269207, 0.15095535, 0.65531915,...  item 3953  108.393867
```

The search will return the data requested in addition to the score of each item.

**Note:** The score is the distance between the query vector and the element. A lower number means that the result is more relevant.

### Filtering (where clause)

You can further filter the elements returned by a search using a where clause.

```python
tbl.search(np.random.random((768))).where("item != 'item 1141'").to_df()
```

### Projections (select clause)

You can select the columns returned by the query using a select clause.

```python
tbl.search(np.random.random((768))).select(["vector"]).to_df()
                                              vector      score
0  [0.30928212, 0.022668175, 0.1756372, 0.4911822...  93.971092
1  [0.2525465, 0.01723831, 0.261568, 0.002007689,...  95.173485
...
```
