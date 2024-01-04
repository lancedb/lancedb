# ANN (Approximate Nearest Neighbor) Indexes

You can create an index over your vector data to make search faster.
Vector indexes are faster but less accurate than exhaustive search (KNN or Flat Search).
LanceDB provides many parameters to fine-tune the index's size, the speed of queries, and the accuracy of results.

Currently, LanceDB does *not* automatically create the ANN index.
LanceDB has optimized code for KNN as well. For many use-cases, datasets under 100K vectors won't require index creation at all.
If you can live with < 100ms latency, skipping index creation is a simpler workflow while guaranteeing 100% recall.

In the future we will look to automatically create and configure the ANN index.

## Types of Index

Lance can support multiple index types, the most widely used one is `IVF_PQ`.

* `IVF_PQ`: use **Inverted File Index (IVF)** to first divide the dataset into `N` partitions,
   and then use **Product Quantization** to compress vectors in each partition.
* `DISKANN` (**Experimental**): organize the vector as a on-disk graph, where the vertices approximately
   represent the nearest neighbors of each vector.

## Creating an IVF_PQ Index

Lance supports `IVF_PQ` index type by default.

=== "Python"
     Creating indexes is done via the [create_index](https://lancedb.github.io/lancedb/python/#lancedb.table.LanceTable.create_index) method.

     ```python
     import lancedb
     import numpy as np
     uri = "data/sample-lancedb"
     db = lancedb.connect(uri)

     # Create 10,000 sample vectors
     data = [{"vector": row, "item": f"item {i}"}
        for i, row in enumerate(np.random.random((10_000, 1536)).astype('float32'))]

     # Add the vectors to a table
     tbl = db.create_table("my_vectors", data=data)

     # Create and train the index - you need to have enough data in the table for an effective training step
     tbl.create_index(num_partitions=256, num_sub_vectors=96)
     ```

=== "Javascript"
     ```javascript
     const vectordb = require('vectordb')
     const db = await vectordb.connect('data/sample-lancedb')

     let data = []
     for (let i = 0; i < 10_000; i++) {
         data.push({vector: Array(1536).fill(i), id: `${i}`, content: "", longId: `${i}`},)
     }
     const table = await db.createTable('my_vectors', data)
     await table.createIndex({ type: 'ivf_pq', column: 'vector', num_partitions: 256, num_sub_vectors: 96 })
     ```

- **metric** (default: "L2"): The distance metric to use. By default it uses euclidean distance "`L2`".
We also support "cosine" and "dot" distance as well.
- **num_partitions** (default: 256): The number of partitions of the index.
- **num_sub_vectors** (default: 96): The number of sub-vectors (M) that will be created during Product Quantization (PQ).
For D dimensional vector, it will be divided into `M` of `D/M` sub-vectors, each of which is presented by
a single PQ code.

<figure markdown>
  ![IVF PQ](./assets/ivf_pq.png)
  <figcaption>IVF_PQ index with <code>num_partitions=2, num_sub_vectors=4</code></figcaption>
</figure>

### Use GPU to build vector index

Lance Python SDK has experimental GPU support for creating IVF index.
Using GPU for index creation requires [PyTorch>2.0](https://pytorch.org/) being installed.

You can specify the GPU device to train IVF partitions via

- **accelerator**: Specify to ``cuda`` or ``mps`` (on Apple Silicon) to enable GPU training.

=== "Linux"

     <!-- skip-test -->
     ``` { .python .copy }
     # Create index using CUDA on Nvidia GPUs.
     tbl.create_index(
          num_partitions=256,
          num_sub_vectors=96,
          accelerator="cuda"
     )
     ```

=== "Macos"

     <!-- skip-test -->
     ```python
     # Create index using MPS on Apple Silicon.
     tbl.create_index(
          num_partitions=256,
          num_sub_vectors=96,
          accelerator="mps"
     )
     ```

Trouble shootings:

If you see ``AssertionError: Torch not compiled with CUDA enabled``, you need to [install
PyTorch with CUDA support](https://pytorch.org/get-started/locally/).


## Querying an ANN Index

Querying vector indexes is done via the [search](https://lancedb.github.io/lancedb/python/#lancedb.table.LanceTable.search) function.

There are a couple of parameters that can be used to fine-tune the search:

- **limit** (default: 10): The amount of results that will be returned
- **nprobes** (default: 20): The number of probes used. A higher number makes search more accurate but also slower.<br/>
  Most of the time, setting nprobes to cover 5-10% of the dataset should achieve high recall with low latency.<br/>
  e.g., for 1M vectors divided up into 256 partitions, nprobes should be set to ~20-40.<br/>
  Note: nprobes is only applicable if an ANN index is present. If specified on a table without an ANN index, it is ignored.
- **refine_factor** (default: None): Refine the results by reading extra elements and re-ranking them in memory.<br/>
  A higher number makes search more accurate but also slower. If you find the recall is less than ideal, try refine_factor=10 to start.<br/>
  e.g., for 1M vectors divided into 256 partitions, if you're looking for top 20, then refine_factor=200 reranks the whole partition.<br/>
  Note: refine_factor is only applicable if an ANN index is present. If specified on a table without an ANN index, it is ignored.

=== "Python"
     ```python
     tbl.search(np.random.random((1536))) \
         .limit(2) \
         .nprobes(20) \
         .refine_factor(10) \
         .to_pandas()
     ```
     ```
                                              vector       item       _distance
     0  [0.44949695, 0.8444449, 0.06281311, 0.23338133...  item 1141  103.575333
     1  [0.48587373, 0.269207, 0.15095535, 0.65531915,...  item 3953  108.393867
     ```

=== "Javascript"
     ```javascript
     const results_1 = await table
         .search(Array(1536).fill(1.2))
         .limit(2)
         .nprobes(20)
         .refineFactor(10)
         .execute()
     ```

The search will return the data requested in addition to the distance of each item.


### Filtering (where clause)

You can further filter the elements returned by a search using a where clause.

=== "Python"
     ```python
     tbl.search(np.random.random((1536))).where("item != 'item 1141'").to_pandas()
     ```

=== "Javascript"
     ```javascript
     const results_2 = await table
         .search(Array(1536).fill(1.2))
         .where("id != '1141'")
         .limit(2)
         .execute()
     ```

### Projections (select clause)

You can select the columns returned by the query using a select clause.

=== "Python"
     ```python
     tbl.search(np.random.random((1536))).select(["vector"]).to_pandas()
     ```
     ```
        vector                                             _distance
     0  [0.30928212, 0.022668175, 0.1756372, 0.4911822...  93.971092
     1  [0.2525465, 0.01723831, 0.261568, 0.002007689,...  95.173485
     ...
     ```

=== "Javascript"
     ```javascript
     const results_3 = await table
         .search(Array(1536).fill(1.2))
         .select(["id"])
         .limit(2)
         .execute()
     ```

## FAQ

### When is it necessary to create an ANN vector index?

`LanceDB` has manually-tuned SIMD code for computing vector distances.
In our benchmarks, computing 100K pairs of 1K dimension vectors takes **less than 20ms**.
For small datasets (< 100K rows) or applications that can accept 100ms latency, vector indices are usually not necessary.

For large-scale or higher dimension vectors, it is beneficial to create vector index.

### How big is my index, and how many memory will it take?

In LanceDB, all vector indices are **disk-based**, meaning that when responding to a vector query, only the relevant pages from the index file are loaded from disk and cached in memory. Additionally, each sub-vector is usually encoded into 1 byte PQ code.

For example, with a 1024-dimension dataset, if we choose `num_sub_vectors=64`, each sub-vector has `1024 / 64 = 16` float32 numbers.
Product quantization can lead to approximately `16 * sizeof(float32) / 1 = 64` times of space reduction.

### How to choose `num_partitions` and `num_sub_vectors` for `IVF_PQ` index?

`num_partitions` is used to decide how many partitions the first level `IVF` index uses.
Higher number of partitions could lead to more efficient I/O during queries and better accuracy, but it takes much more time to train.
On `SIFT-1M` dataset, our benchmark shows that keeping each partition 1K-4K rows lead to a good latency / recall.

`num_sub_vectors` specifies how many Product Quantization (PQ) short codes to generate on each vector. Because
PQ is a lossy compression of the original vector, a higher `num_sub_vectors` usually results in
less space distortion, and thus yields better accuracy. However, a higher `num_sub_vectors` also causes heavier I/O and
more PQ computation, and thus, higher latency. `dimension / num_sub_vectors` should be a multiple of 8 for optimum SIMD efficiency.