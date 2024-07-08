# Llama-Index
![Illustration](../assets/llama-index.jpg)

## Quick start
You would need to install the integration via `pip install llama-index-vector-stores-lancedb` in order to use it. 
You can run the below script to try it out :
```python
import logging
import sys

# Uncomment to see debug logs
# logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
# logging.getLogger().addHandler(logging.StreamHandler(stream=sys.stdout))

from llama_index.core import SimpleDirectoryReader, Document, StorageContext
from llama_index.core import VectorStoreIndex
from llama_index.vector_stores.lancedb import LanceDBVectorStore
import textwrap
import openai

openai.api_key = "sk-..."

documents = SimpleDirectoryReader("./data/your-data-dir/").load_data()
print("Document ID:", documents[0].doc_id, "Document Hash:", documents[0].hash)

## For LanceDB cloud :
# vector_store = LanceDBVectorStore( 
#     uri="db://db_name", # your remote DB URI
#     api_key="sk_..", # lancedb cloud api key
#     region="your-region" # the region you configured
#     ...
# )

vector_store = LanceDBVectorStore(
    uri="./lancedb", mode="overwrite", query_type="vector"
)
storage_context = StorageContext.from_defaults(vector_store=vector_store)

index = VectorStoreIndex.from_documents(
    documents, storage_context=storage_context
)
lance_filter = "metadata.file_name = 'paul_graham_essay.txt' "
retriever = index.as_retriever(vector_store_kwargs={"where": lance_filter})
response = retriever.retrieve("What did the author do growing up?")
```

Checkout Complete example here - [LlamaIndex demo](../notebooks/LlamaIndex_example.ipynb)

### Filtering
For metadata filtering, you can use a Lance SQL-like string filter as demonstrated in the example above. Additionally, you can also filter using the `MetadataFilters` class from LlamaIndex:
```python
from llama_index.core.vector_stores import (
    MetadataFilters,
    FilterOperator,
    FilterCondition,
    MetadataFilter,
)

query_filters = MetadataFilters(
    filters=[
        MetadataFilter(
            key="creation_date", operator=FilterOperator.EQ, value="2024-05-23"
        ),
        MetadataFilter(
            key="file_size", value=75040, operator=FilterOperator.GT
        ),
    ],
    condition=FilterCondition.AND,
)
```

### Hybrid Search
For complete documentation, refer [here](https://lancedb.github.io/lancedb/hybrid_search/hybrid_search/). This example uses the `colbert` reranker. Make sure to install necessary dependencies for the reranker you choose.
```python
from lancedb.rerankers import ColbertReranker

reranker = ColbertReranker()
vector_store._add_reranker(reranker)

query_engine = index.as_query_engine(
    filters=query_filters,
    vector_store_kwargs={
        "query_type": "hybrid",
    }
)

response = query_engine.query("How much did Viaweb charge per month?")
```

In the above snippet, you can change/specify query_type again when creating the engine/retriever.

## API reference
The exhaustive list of parameters for `LanceDBVectorStore` vector store are :  
- `connection`: Optional, `lancedb.db.LanceDBConnection` connection object to use. If not provided, a new connection will be created.
- `uri`: Optional[str], the uri of your database. Defaults to `"/tmp/lancedb"`.
- `table_name` : Optional[str], Name of your table in the database. Defaults to `"vectors"`.
- `table`: Optional[Any], `lancedb.db.LanceTable` object to be passed. Defaults to `None`. 
- `vector_column_name`: Optional[Any], Column name to use for vector's in the table. Defaults to `'vector'`.   
- `doc_id_key`: Optional[str], Column name to use for document id's in the table. Defaults to `'doc_id'`.  
- `text_key`: Optional[str], Column name to use for text in the table. Defaults to `'text'`.  
- `api_key`: Optional[str], API key to use for LanceDB cloud database. Defaults to `None`.  
- `region`: Optional[str], Region to use for LanceDB cloud database. Only for LanceDB Cloud, defaults to `None`.  
- `nprobes` : Optional[int], Set the number of probes to use. Only applicable if ANN index is created on the table else its ignored. Defaults to `20`.
- `refine_factor` : Optional[int], Refine the results by reading extra elements and re-ranking them in memory. Defaults to `None`.
- `reranker`: Optional[Any], The reranker to use for LanceDB.
        Defaults to `None`.
- `overfetch_factor`: Optional[int], The factor by which to fetch more results.
        Defaults to `1`.
- `mode`: Optional[str], The mode to use for LanceDB.
            Defaults to `"overwrite"`.
- `query_type`:Optional[str], The type of query to use for LanceDB.
            Defaults to `"vector"`.


### Methods 

- __from_table(cls, table: lancedb.db.LanceTable) -> `LanceDBVectorStore`__ : (class method) Creates instance from lancedb table. 

- **_add_reranker(self, reranker: lancedb.rerankers.Reranker) -> `None`** : Add a reranker to an existing vector store. 
    - Usage :
        ```python
        from lancedb.rerankers import ColbertReranker
        reranker = ColbertReranker()
        vector_store._add_reranker(reranker)
        ```               
- **_table_exists(self, tbl_name: `Optional[str]` = `None`) -> `bool`** : Returns `True` if `tbl_name` exists in database.
- __create_index(  
  self, scalar: `Optional[bool]` = False, col_name: `Optional[str]` = None, num_partitions: `Optional[int]` = 256, num_sub_vectors: `Optional[int]` = 96, index_cache_size: `Optional[int]` = None, metric: `Optional[str]` = "L2",  
) -> `None`__ : Creates a scalar(for non-vector cols) or a vector index on a table.
        Make sure your vector column has enough data before creating an index on it.

- __add(self, nodes: `List[BaseNode]`, **add_kwargs: `Any`, ) -> `List[str]`__ :
adds Nodes to the table

- **delete(self, ref_doc_id: `str`) -> `None`**: Delete nodes using with node_ids.
- **delete_nodes(self, node_ids: `List[str]`) -> `None`** : Delete nodes using with node_ids.
- __query(
        self,
        query: `VectorStoreQuery`,
        **kwargs: `Any`,
    ) -> `VectorStoreQueryResult`__:
        Query index(`VectorStoreIndex`) for top k most similar nodes. Accepts llamaIndex `VectorStoreQuery` object.