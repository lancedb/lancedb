# Late interaction & MultiVector embedding type
Late interaction is a technique used in retrieval that calculates the relevance of a query to a document by comparing their multi-vector representations. The key difference between late interaction and other popular methods:

![late interaction vs other methods](https://raw.githubusercontent.com/lancedb/assets/b035a0ceb2c237734e0d393054c146d289792339/docs/assets/integration/colbert-blog-interaction.svg)


[ Illustration from https://jina.ai/news/what-is-colbert-and-late-interaction-and-why-they-matter-in-search/]

<b>No interaction:</b> Refers to independently embedding the query and document, that are compared to calcualte similarity without any interaction between them. This is typically used in vector search operations.

<b>Partial interaction</b> Refers to a specific approach where the similarity computation happens primarily between query vectors and document vectors, without extensive interaction between individual components of each. An example of this is dual-encoder models like BERT.

<b>Early full interaction</b> Refers to techniques like cross-encoders that process query and docs in pairs with full interaction across various stages of encoding. This is a powerful, but relatively slower technique. Because it requires processing query and docs in pairs, doc embeddings can't be pre-computed for fast retrieval. This is why cross encoders are typically used as reranking models combined with vector search. Learn more about [LanceDB Reranking support](https://lancedb.github.io/lancedb/reranking/).

<b>Late interaction</b> Late interaction is a technique that calculates the doc and query similarity independently and then the interaction or evaluation happens during the retrieval process. This is typically used in retrieval models like ColBERT. Unlike early interaction, It allows speeding up the retrieval process without compromising the depth of semantic analysis.

## Internals of ColBERT 
Let's take a look at the steps involved in performing late interaction based retrieval using ColBERT:

• ColBERT employs BERT-based encoders for both queries `(fQ)` and documents `(fD)`
• A single BERT model is shared between query and document encoders and special tokens distinguish input types: `[Q]` for queries and `[D]` for documents

**Query Encoder (fQ):**
• Query q is tokenized into WordPiece tokens: `q1, q2, ..., ql`. `[Q]` token is prepended right after BERT's `[CLS]` token
• If query length < Nq, it's padded with [MASK] tokens up to Nq.
• The padded sequence goes through BERT's transformer architecture
• Final embeddings are L2-normalized.

**Document Encoder (fD):**
• Document d is tokenized into tokens `d1, d2, ..., dm`. `[D]` token is prepended after `[CLS]` token
• Unlike queries, documents are NOT padded with `[MASK]` tokens
• Document tokens are processed through BERT and the same linear layer

**Late Interaction:**
• Late interaction estimates relevance score `S(q,d)` using embedding `Eq` and `Ed`. Late interaction happens after independent encoding
• For each query embedding, maximum similarity is computed against all document embeddings
• The similarity measure can be cosine similarity or squared L2 distance

**MaxSim Calculation:**
```
S(q,d) := Σ max(Eqi⋅EdjT)
          i∈|Eq| j∈|Ed|
```
• This finds the best matching document embedding for each query embedding
• Captures relevance based on strongest local matches between contextual embeddings

## LanceDB MultiVector type
LanceDB supports multivector type, this is useful when you have multiple vectors for a single item (e.g. with ColBert and ColPali).

You can index on a column with multivector type and search on it, the query can be single vector or multiple vectors. For now, only cosine metric is supported for multivector search. The vector value type can be float16, float32 or float64. LanceDB integrateds [ConteXtualized Token Retriever(XTR)](https://arxiv.org/abs/2304.01982), which introduces a simple, yet novel, objective function that encourages the model to retrieve the most important document tokens first. 

```python
import lancedb
import numpy as np
import pyarrow as pa

db = lancedb.connect("data/multivector_demo")
schema = pa.schema(
    [
        pa.field("id", pa.int64()),
        # float16, float32, and float64 are supported
        pa.field("vector", pa.list_(pa.list_(pa.float32(), 256))),
    ]
)
data = [
    {
        "id": i,
        "vector": np.random.random(size=(2, 256)).tolist(),
    }
    for i in range(1024)
]
tbl = db.create_table("my_table", data=data, schema=schema)

# only cosine similarity is supported for multi-vectors
tbl.create_index(metric="cosine")

# query with single vector
query = np.random.random(256).astype(np.float16)
tbl.search(query).to_arrow()

# query with multiple vectors
query = np.random.random(size=(2, 256))
tbl.search(query).to_arrow()
```
Find more about vector search in LanceDB [here](https://lancedb.github.io/lancedb/search/#multivector-type).
