
# Getting Started with LanceDB: A Minimal Vector Search Tutorial

Let's set up a LanceDB database, insert vector data, and perform a simple vector search. We'll use simple character classes like "knight" and "rogue" to illustrate semantic relevance.

## 1. Install Dependencies

Before starting, make sure you have the necessary packages:

```bash
pip install lancedb pandas numpy
```

## 2. Import Required Libraries

```python
import lancedb
import pandas as pd
import numpy as np
```

## 3. Connect to LanceDB

You can use a local directory to store your database:

```python
db = lancedb.connect("./lancedb")
```

## 4. Create Sample Data

Add sample text data corresponding 4D vectors:

```python
data = pd.DataFrame([
    {"id": "1", "vector": [1.0, 0.0, 0.0, 0.0], "text": "knight"},
    {"id": "2", "vector": [0.9, 0.1, 0.0, 0.0], "text": "warrior"},
    {"id": "3", "vector": [0.0, 1.0, 0.0, 0.0], "text": "rogue"},
    {"id": "4", "vector": [0.0, 0.9, 0.1, 0.0], "text": "thief"},
    {"id": "5", "vector": [0.5, 0.5, 0.0, 0.0], "text": "ranger"},
])
```

## 5. Create a Table in LanceDB

```python
table = db.create_table("rpg_classes", data=data, mode="overwrite")
```

Let's see how the table looks:
```python
print(data)
```

| id | vector | text |
|----|--------|------|
| 1 | [1.0, 0.0, 0.0, 0.0] | knight |
| 2 | [0.9, 0.1, 0.0, 0.0] | warrior |
| 3 | [0.0, 1.0, 0.0, 0.0] | rogue |
| 4 | [0.0, 0.9, 0.1, 0.0] | thief |
| 5 | [0.5, 0.5, 0.0, 0.0] | ranger |



## 6. Perform a Vector Search

Search for the most similar character classes to our query vector:

```python
# Query as if we are searching for "rogue"
results = table.search([0.95, 0.05, 0.0, 0.0]).limit(3).to_df()
print(results)
```

This will return the top 3 closest classes to the vector, effectively showing how LanceDB can be used for semantic search.

|  id  |          vector         |   text   | _distance |
|------|------------------------|----------|-----------|
|  3   | [0.0, 1.0, 0.0, 0.0]  |  rogue   |   0.00   |
|  4   | [0.0, 0.9, 0.1, 0.0]  | thief |   0.02   |
|  5   | [0.5, 0.5, 0.0, 0.0]  |  ranger  |   0.50   |

Let's try searching for "knight"

```python
query_vector = [1.0, 0.0, 0.0, 0.0]
results = table.search(query_vector).limit(3).to_pandas()
print(results)
```

|  id  |          vector         |   text   | _distance |
|------|------------------------|----------|-----------|
|  1   | [1.0, 0.0, 0.0, 0.0]  |  knight  |   0.00   |
|  2   | [0.9, 0.1, 0.0, 0.0]  | warrior  |   0.02   |
|  5   | [0.5, 0.5, 0.0, 0.0]  |  ranger  |   0.50   |

## Next Steps

That's it - you just conducted vector search!

For more beginner tips, check out the [Basic Usage](basic.md) guide.
