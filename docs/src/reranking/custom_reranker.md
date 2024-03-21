## Building Custom Rerankers
You can build your own custom reranker by subclassing the `Reranker` class and implementing the `rerank_hybrid()` method. Optionally, you can also implement the `rerank_vector()` and `rerank_fts()` methods if you want to support reranking for vector and FTS search separately.
Here's an example of a custom reranker that combines the results of semantic and full-text search using a linear combination of the scores.

The `Reranker` base interface comes with a `merge_results()` method that can be used to combine the results of semantic and full-text search. This is a vanilla merging algorithm that simply concatenates the results and removes the duplicates without taking the scores into consideration. It only keeps the first copy of the row encountered. This works well in cases that don't require the scores of semantic and full-text search to combine the results. If you want to use the scores or want to support `return_score="all"`, you'll need to implement your own merging algorithm.

```python

from lancedb.rerankers import Reranker
import pyarrow as pa

class MyReranker(Reranker):
    def __init__(self, param1, param2, ..., return_score="relevance"):
        super().__init__(return_score)
        self.param1 = param1
        self.param2 = param2
    
    def rerank_hybrid(self, query: str, vector_results: pa.Table, fts_results: pa.Table):
        # Use the built-in merging function
        combined_result = self.merge_results(vector_results, fts_results)
        
        # Do something with the combined results
        # ...

        # Return the combined results
        return combined_result
    
    def rerank_vector(self, query: str, vector_results: pa.Table):
        # Do something with the vector results
        # ...

        # Return the vector results
        return vector_results
    
    def rerank_fts(self, query: str, fts_results: pa.Table):
        # Do something with the FTS results
        # ...

        # Return the FTS results
        return fts_results

```

### Example of a Custom Reranker
For the sake of simplicity let's build custom reranker that just enchances the Cohere Reranker by accepting a filter query, and accept other CohereReranker params as kwags.

```python

from typing import List, Union
import pandas as pd
from lancedb.rerankers import CohereReranker

class ModifiedCohereReranker(CohereReranker):
    def __init__(self, filters: Union[str, List[str]], **kwargs):
        super().__init__(**kwargs)
        filters = filters if isinstance(filters, list) else [filters]
        self.filters = filters

    def rerank_hybrid(self, query: str, vector_results: pa.Table, fts_results: pa.Table)-> pa.Table:
        combined_result = super().rerank_hybrid(query, vector_results, fts_results)
        df = combined_result.to_pandas()
        for filter in self.filters:
            df = df.query("not text.str.contains(@filter)")

        return pa.Table.from_pandas(df)
    
    def rerank_vector(self, query: str, vector_results: pa.Table)-> pa.Table:
        vector_results = super().rerank_vector(query, vector_results)
        df = vector_results.to_pandas()
        for filter in self.filters:
            df = df.query("not text.str.contains(@filter)")

        return pa.Table.from_pandas(df)
    
    def rerank_fts(self, query: str, fts_results: pa.Table)-> pa.Table:
        fts_results = super().rerank_fts(query, fts_results)
        df = fts_results.to_pandas()
        for filter in self.filters:
            df = df.query("not text.str.contains(@filter)")

        return pa.Table.from_pandas(df)

```

!!! tip
    The `vector_results` and `fts_results` are pyarrow tables. Lean more about pyarrow tables [here](https://arrow.apache.org/docs/python). It can be convered to other data types like pandas dataframe, pydict, pylist etc.

    For example, You can convert them to pandas dataframes using `to_pandas()` method and perform any operations you want. After you are done, you can convert the dataframe back to pyarrow table using `pa.Table.from_pandas()` method and return it.

