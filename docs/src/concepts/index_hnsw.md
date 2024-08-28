
# Understanding HNSW index

Approximate Nearest Neighbor (ANN) search is a method for finding data points near a given point in a dataset, though not always the exact nearest one. HNSW is one of the most accurate and fastest Approximate Nearest Neighbour search algorithms, It’s beneficial in high-dimensional spaces where finding the same nearest neighbor would be too slow and costly

[Jump to usage](#usage)
There are three main types of ANN search algorithms:

* **Tree-based search algorithms**: Use a tree structure to organize and store data points.
* **Hash-based search algorithms**: Use a hash table to store and manage data points.
* **Graph-based search algorithms**: Use a graph structure to store data points, which can be a bit complex. 

HNSW is a graph-based algorithm. All graph-based search algorithms rely on the idea of a proximity graph, where the graph is built based on the proximity of data points, measured by their Euclidean distances.

Two important algorithms that help understand HNSW: the Skip List and Navigable Small World (NSW) Graphs, which are the predecessors of HNSW.

## Navigable Small World (NSW) Graphs
The main concept behind NSW graphs is that if we take proximity graphs with long-range and short-range connections, search times can be very fast.

* Each vertex/node connects to several others, forming a "friend list" containing nodes connected to it making search faster.
* To search, we start at a set entry point and move to the closest neighbor.
* We continue moving to the nearest neighbor in each friend list until we can't find a closer one.
This method makes finding a target much quicker.

![](../assets/nsw.png)

Routing or Navigation through the graph happens in two steps:

* **Zoom-Out Phase**: We first move through vertices/nodes with fewer connections to cover a lot of ground quickly, this step makes it faster.
* **Zoom-In Phase**: Next, we move through vertices with more connections to get closer to our target.
![](../assets/nsw_routing.png)

## Skip List
Skip List allows fast search capabilities similar to a sorted array but also enables quick element insertion, unlike sorted arrays.  In simple words, the skip list is a multilayer linked list that stores a sorted list of elements where the top layer has links that skip over many nodes, while each lower layer skips over fewer nodes.
To search in the skip list, we start at the top layer with the longest skips and move right. If you find that the current node’s key is greater than the one you're looking for, move down to the next level and continue searching.

![](../assets/skip_list.png)

In the image above, the blue nodes represent the nodes that are compared to find our target value of 17, and the yellow node represents the found target.
HNSW also uses a similar layered structure, with high-degree nodes in the top layers for quick searches and low-degree nodes in the lower layers for more accurate searches.

## HNSW: Hierarchical Navigable Small Worlds

HNSW improved the NSW algorithm by adding a hierarchical structure using a skip list. Adding hierarchy to NSW forms a layer graph where at the top layer, we have the longest links, and at the bottom layer, we have the shortest.

![](../assets/hnsw.png)

During the search in HNSW, we
1. Start at the top layer with the longest links, these vertices usually have longer connections and spread across layers.
2. Move to the nearest higher-degree vertices in each layer until reaching a local minimum.
3. Switch to a lower layer and repeat the process.
4. Continue this until the local minimum in the bottom layer (layer 0) is found.

![](../assets/hnsw_2.png)

## Usage

We can combine the above concepts to understand how to build and query an HNSW index in LanceDB.

### Construct index

```python
import lancedb
import numpy as np
uri = "/tmp/lancedb"
db = lancedb.connect(uri)

# Create 10,000 sample vectors
data = [{"vector": row, "item": f"item {i}"}
   for i, row in enumerate(np.random.random((10_000, 1536)).astype('float32'))]

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
