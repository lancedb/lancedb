# ANN (Approximate Nearest Neighbor) Indexes

In order to make vector search faster, you can create an index over your vector data. Vector indexes are faster but
not as accurate as exhaustive search. LanceDB provide many parameters to fine tune the size of the index, the speed
of queries and the accuracy for results

## Creating an ANN Index

This is how you can create a vector index:

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
tbl.create_index(num_partitions=256,num_sub_vectors=96)
```

Since `create_index` has a training, it can take a couple of minutes to finish for large tables. You can control index
creation by providing the following parameters:
- **num_partitions**: The number of partitions of the index. A higher number leads to better search quality, but it makes index 
generation slower.
- **num_partitions**: The number of subvectors (M) that will be created during Product Quantization (PQ). A larger number makes
search more accurate, but also makes the index larger and slower to build. 

## Querying an ANN Index

TODO