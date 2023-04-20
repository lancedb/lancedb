# ANN (Approximate Nearest Neighbor) Indexes

You can create an index over your vector data to make search faster. Vector indexes are faster but less accurate than exhaustive search. LanceDB provides many parameters to fine-tune the index's size, the speed of queries, and the accuracy of results.

Currently, LanceDB does not automatically create the ANN index. In the future we will look to improve this experience and automate index creation and configuration.

## Creating an ANN Index

Creating indexes is done via the [create_index](https://lancedb.github.io/lancedb/python/#lancedb.table.LanceTable.create_index) function.

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
- **nprobes** (default: 20): The number of probes used. A higher number makes search more accurate but also slower.
- **refine_factor** (default: None): Refine the results by reading extra elements and re-ranking them in memory. A higher number makes 
search more accurate but also slower.

```python
tbl.search(np.random.random((768))) \
    .limit(2) \
    .nprobes(20) \
    .refine_factor(20) \
    .to_df()

                                              vector       item       score
0  [0.44949695, 0.8444449, 0.06281311, 0.23338133...  item 1141  103.575333
1  [0.48587373, 0.269207, 0.15095535, 0.65531915,...  item 3953  108.393867
```

The search will return the data requested in addition to the score of each item. The score is the distance between the query vector and the element. A lower number means that the result is more relevant.
