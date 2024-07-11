## Improving retriever performance

Try it yourself - <a href="https://colab.research.google.com/github/lancedb/lancedb/blob/main/docs/src/notebooks/lancedb_reranking.ipynb"><img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Open In Colab"></a><br/>

VectorDBs are used as retreivers in recommender or chatbot-based systems for retrieving relevant data based on user queries. For example, retriever is a critical component of Retrieval Augmented Generation (RAG) acrhitectures. In this section, we will discuss how to improve the performance of retrievers.

There are serveral ways to improve the performance of retrievers. Some of the common techniques are:

* Using different query types
* Using hybrid search
* Fine-tuning the embedding models
* Using different embedding models

Using different embedding models is something that's very specific to the use case and the data. So we will not discuss it here. In this section, we will discuss the first three techniques.


!!! note "Note"
    We'll be using a simple metric called "hit-rate" for evaluating the performance of the retriever across this guide. Hit-rate is the percentage of queries for which the retriever returned the correct answer in the top-k results. For example, if the retriever returned the correct answer in the top-3 results for 70% of the queries, then the hit-rate@3 is 0.7.


## The dataset
We'll be using a QA dataset generated using a LLama2 review paper. The dataset contains 221 query, context and answer triplets. The queries and answers are generated using GPT-4 based on a given query. Full script used to generate the dataset can be found on this [repo](https://github.com/lancedb/ragged). It can be downloaded from [here](https://github.com/AyushExel/assets/blob/main/data_qa.csv)

### Using different query types
Let's setup the embeddings and the dataset first. We'll use the LanceDB's `huggingface` embeddings integration for this guide. 

```python
import lancedb
import pandas as pd
from lancedb.embeddings import get_registry
from lancedb.pydantic import Vector, LanceModel

db = lancedb.connect("~/lancedb/query_types")
df = pd.read_csv("data_qa.csv")

embed_fcn = get_registry().get("huggingface").create(name="BAAI/bge-small-en-v1.")

class Schema(LanceModel):
    context: str = embed_fcn.SourceField()
    vector: Vector(embed_fcn.ndims()) = embed_fcn.VectorField()

table = db.create_table("qa", schema=Schema)
table.add(df[["context"]].to_dict(orient="records"))

queries = df["query"].tolist()
```

Now that we have the dataset and embeddings table set up, here's how you can run different query types on the dataset.

* <b> Vector Search: </b>

    ```python
    table.search(quries[0], query_type="vector").limit(5).to_pandas()
    ```
    By default, LanceDB uses vector search query type for searching and it automatically converts the input query to a vector before searching when using embedding API. So, the following statement is equivalent to the above statement.

    ```python
    table.search(quries[0]).limit(5).to_pandas()
    ```

    Vector or semantic search is useful when you want to find documents that are similar to the query in terms of meaning.

---

* <b> Full-text Search: </b>
    
    FTS requires creating an index on the column you want to search on. `replace=True` will replace the existing index if it exists.
    Once the index is created, you can search using the `fts` query type.
    ```python
    table.create_fts_index("context", replace=True)
    table.search(quries[0], query_type="fts").limit(5).to_pandas()
    ```

    Full-text search is useful when you want to find documents that contain the query terms.

---

* <b> Hybrid Search: </b>

    Hybrid search is a combination of vector and full-text search. Here's how you can run a hybrid search query on the dataset.
    ```python
    table.search(quries[0], query_type="hybrid").limit(5).to_pandas()
    ```
    Hybrid search requires a reranker to combine and rank the results from vector and full-text search. We'll cover reranking as a concept in the next section.

    Hybrid search is useful when you want to combine the benefits of both vector and full-text search.

    !!! note "Note"
        By default, it uses `LinearCombinationReranker` that combines the scores from vector and full-text search using a weighted linear combination. It is the simplest reranker implementation available in LanceDB. You can also use other rerankers like `CrossEncoderReranker` or `CohereReranker` for reranking the results.
        Learn more about rerankers [here](https://lancedb.github.io/lancedb/reranking/)

    

### Hit rate evaluation results

Now that we have seen how to run different query types on the dataset, let's evaluate the hit-rate of each query type on the dataset.
For brevity, the entire evaluation script is not shown here. You can find the complete evaluation and benchmarking utility scripts [here](https://github.com/lancedb/ragged).

Here are the hit-rate results for the dataset:

| Query Type | Hit-rate@5 |
| --- | --- |
| Vector Search | 0.640 |
| Full-text Search | 0.595 |
| Hybrid Search (w/ LinearCombinationReranker) | 0.645 |

**Choosing query type** is very specific to the use case and the data. This synthetic dataset has been generated to be semantically challenging, i.e, the queries don't have a lot of keywords in common with the context. So, vector search performs better than full-text search. However, in real-world scenarios, full-text search might perform better than vector search. Hybrid search is a good choice when you want to combine the benefits of both vector and full-text search.

### Evaluation results on other datasets

The hit-rate results can vary based on the dataset and the query type. Here are the hit-rate results for the other datasets using the same embedding function.

* <b> SQuAD Dataset: </b>

    | Query Type | Hit-rate@5 |
    | --- | --- |
    | Vector Search | 0.822 |
    | Full-text Search | 0.835 |
    | Hybrid Search (w/ LinearCombinationReranker) | 0.8874 |

* <b> Uber10K sec filing Dataset: </b>

    | Query Type | Hit-rate@5 |
    | --- | --- |
    | Vector Search | 0.608 |
    | Full-text Search | 0.82 |
    | Hybrid Search (w/ LinearCombinationReranker) | 0.80 |

In these standard datasets, FTS seems to perform much better than vector search because the queries have a lot of keywords in common with the context. So, in general choosing the query type is very specific to the use case and the data.


