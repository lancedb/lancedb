
# Understanding HNSW index

Approximate Nearest Neighbor (ANN) search is a method for finding data points near a given point in a dataset, though not always the exact nearest one. HNSW is one of the most accurate and fastest Approximate Nearest Neighbour search algorithms, It’s beneficial in high-dimensional spaces where finding the same nearest neighbor would be too slow and costly

[Jump to usage](#usage)
There are three main types of ANN search algorithms:

* **Tree-based search algorithms**: Use a tree structure to organize and store data points.
* * **Hash-based search algorithms**: Use a specialized geometric hash table to store and manage data points. These algorithms typically focus on theoretical guarantees, and don't usually perform as well as the other approaches in practice.
* **Graph-based search algorithms**: Use a graph structure to store data points, which can be a bit complex. 

HNSW is a graph-based algorithm. All graph-based search algorithms rely on the idea of a k-nearest neighbor (or k-approximate nearest neighbor) graph, which we outline below.  
HNSW also combines this with the ideas behind a classic 1-dimensional search data structure: the skip list.

## k-Nearest Neighbor Graphs and k-approximate Nearest neighbor Graphs
The k-nearest neighbor graph actually predates its use for ANN search. Its construction is quite simple:
* Each vector in the dataset is given an associated vertex.
* Each vertex has outgoing edges to its k nearest neighbors. That is, the k closest other vertices by Euclidean distance between the two corresponding vectors. This can be thought of as a "friend list" for the vertex.
* For some applications (including nearest-neighbor search), the incoming edges are also added.

Eventually, it was realized that the following greedy search method over such a graph typically results in good approximate nearest neighbors:
* Given a query vector, start at some fixed "entry point" vertex (e.g. the approximate center node).
* Look at that vertex's neighbors. If any of them are closer to the query vector than the current vertex, then move to that vertex.
* Repeat until a local optimum is found.

The above algorithm also generalizes to e.g. top 10 approximate nearest neighbors.

Computing a k-nearest neighbor graph is actually quite slow, taking quadratic time in the dataset size. It was quickly realized that near-identical performance can be achieved using a k-approximate nearest neighbor graph. That is, instead of obtaining the k-nearest neighbors for each vertex, an approximate nearest neighbor search data structure is used to build much faster.  
In fact, another data structure is not needed: This can be done "incrementally".
That is, if you start with a k-ANN graph for n-1 vertices, you can extend it to a k-ANN graph for n vertices as well by using the graph to obtain the k-ANN for the new vertex.

One downside of k-NN and k-ANN graphs alone is that one must typically build them with a large value of k to get decent results, resulting in a large index.


## HNSW: Hierarchical Navigable Small Worlds

HNSW builds on k-ANN in two main ways:
* Instead of getting the k-approximate nearest neighbors for a large value of k, it sparsifies the k-ANN graph using a carefully chosen "edge pruning" heuristic, allowing for the number of edges per vertex to be limited to a relatively small constant.
* The "entry point" vertex is chosen dynamically using a recursively constructed data structure on a subset of the data, similarly to a skip list.

This recursive structure can be thought of as separating into layers:
* At the bottom-most layer, an k-ANN graph on the whole dataset is present.
* At the second layer, a k-ANN graph on a fraction of the dataset (e.g. 10%) is present.
* At the Lth layer, a k-ANN graph is present. It is over a (constant) fraction (e.g. 10%) of the vectors/vertices present in the L-1th layer.

Then the greedy search routine operates as follows:
* At the top layer (using an arbitrary vertex as an entry point), use the greedy local search routine on the k-ANN graph to get an approximate nearest neighbor at that layer.
* Using the approximate nearest neighbor found in the previous layer as an entry point, find an approximate nearest neighbor in the next layer with the same method.
* Repeat until the bottom-most layer is reached. Then use the entry point to find multiple nearest neighbors (e.g. top 10).


## Usage

We can combine the above concepts to understand how to build and query an HNSW index in LanceDB.

### Construct index

```python
import lancedb
import numpy as np
uri = "/tmp/lancedb"
db = lancedb.connect(uri)

# Create 10,000 sample vectors
data = [
    {"vector": row, "item": f"item {i}"}
    for i, row in enumerate(np.random.random((10_000, 1536)).astype('float32'))
]

# Add the vectors to a table
tbl = db.create_table("my_vectors", data=data)

# Create and train the HNSW index for a 1536-dimensional vector
# Make sure you have enough data in the table for an effective training step
tbl.create_index(index_type=IVF_HNSW_SQ)

```

### Query the index

```python
# Search using a random 1536-dimensional embedding
tbl.search(np.random.random((1536))) \
    .limit(2) \
    .to_pandas()
```
