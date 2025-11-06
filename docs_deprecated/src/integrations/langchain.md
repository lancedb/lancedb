**LangChain** is a framework designed for building applications with large language models (LLMs) by chaining together various components. It supports a range of functionalities including memory, agents, and chat models, enabling developers to create context-aware applications.

![Illustration](https://raw.githubusercontent.com/lancedb/assets/refs/heads/main/docs/assets/integration/langchain_rag.png)

LangChain streamlines these stages (in figure above) by providing pre-built components and tools for integration, memory management, and deployment, allowing developers to focus on application logic rather than underlying complexities.

Integration of **Langchain** with **LanceDB** enables applications to retrieve the most relevant data by comparing query vectors against stored vectors, facilitating effective information retrieval. It results in better and context aware replies and actions by the LLMs.

## Quick Start
You can load your document data using langchain's loaders, for this example we are using `TextLoader` and `OpenAIEmbeddings` as the embedding model. Checkout Complete example here - [LangChain demo](../notebooks/langchain_example.ipynb)
```python
import os
from langchain.document_loaders import TextLoader
from langchain.vectorstores import LanceDB
from langchain_openai import OpenAIEmbeddings
from langchain_text_splitters import CharacterTextSplitter

os.environ["OPENAI_API_KEY"] = "sk-..."

loader = TextLoader("../../modules/state_of_the_union.txt") # Replace with your data path
documents = loader.load()

documents = CharacterTextSplitter().split_documents(documents)
embeddings = OpenAIEmbeddings()

docsearch = LanceDB.from_documents(documents, embeddings)
query = "What did the president say about Ketanji Brown Jackson"
docs = docsearch.similarity_search(query)
print(docs[0].page_content)
```

## Documentation
In the above example `LanceDB` vector store class object is created using `from_documents()` method  which is a `classmethod` and returns the initialized class object. 

You can also use `LanceDB.from_texts(texts: List[str],embedding: Embeddings)` class method.  

The exhaustive list of parameters for `LanceDB` vector store are : 

|Name|type|Purpose|default|
|:----|:----|:----|:----|
|`connection`| (Optional) `Any` |`lancedb.db.LanceDBConnection` connection object to use.  If not provided, a new connection will be created.|`None`|
|`embedding`| (Optional) `Embeddings` | Langchain embedding model.|Provided by user.|
|`uri`| (Optional) `str` |It specifies the directory location of **LanceDB database** and establishes a connection that can be used to interact with the database. |`/tmp/lancedb`|
|`vector_key` |(Optional) `str`| Column name to use for vector's in the table.|`'vector'`|
|`id_key` |(Optional) `str`| Column name to use for id's in the table.|`'id'`|
|`text_key` |(Optional) `str` |Column name to use for text in the table.|`'text'`|
|`table_name` |(Optional) `str`| Name of your table in the database.|`'vectorstore'`|
|`api_key` |(Optional `str`) |API key to use for LanceDB cloud database.|`None`|
|`region` |(Optional) `str`| Region to use for LanceDB cloud database.|Only for LanceDB Cloud : `None`.|
|`mode` |(Optional) `str` |Mode to use for adding data to the table. Valid values are "append" and "overwrite".|`'overwrite'`|
|`table`| (Optional) `Any`|You can connect to an existing table of LanceDB, created outside of langchain, and utilize it.|`None`|
|`distance`|(Optional) `str`|The choice of distance metric used to calculate the similarity between vectors.|`'l2'`|
|`reranker` |(Optional) `Any`|The reranker to use for LanceDB.|`None`|
|`relevance_score_fn` |(Optional) `Callable[[float], float]` | Langchain relevance score function to be used.|`None`|
|`limit`|`int`|Set the maximum number of results to return.|`DEFAULT_K` (it is 4)|

```python
db_url = "db://lang_test" # url of db you created
api_key = "xxxxx" # your API key
region="us-east-1-dev"  # your selected region

vector_store = LanceDB(
    uri=db_url,
    api_key=api_key, #(dont include for local API)
    region=region, #(dont include for local API)
    embedding=embeddings,
    table_name='langchain_test' # Optional
    )
```

### Methods 

##### add_texts()

This method turn texts into embedding and add it to the database.

|Name|Purpose|defaults|
|:---|:---|:---|
|`texts`|`Iterable` of strings to add to the vectorstore.|Provided by user|
|`metadatas`|Optional `list[dict()]` of metadatas associated with the texts.|`None`|
|`ids`|Optional `list` of ids to associate with the texts.|`None`|
|`kwargs`| Other keyworded arguments provided by the user. |-|

It returns list of ids of the added texts.

```python
vector_store.add_texts(texts = ['test_123'], metadatas =[{'source' :'wiki'}]) 

#Additionaly, to explore the table you can load it into a df or save it in a csv file:

tbl = vector_store.get_table()
print("tbl:", tbl)
pd_df = tbl.to_pandas()
pd_df.to_csv("docsearch.csv", index=False)

# you can also create a new vector store object using an older connection object:
vector_store = LanceDB(connection=tbl, embedding=embeddings)
```

------


##### create_index() 

This method creates a scalar(for non-vector cols) or a vector index on a table.

|Name|type|Purpose|defaults|
|:---|:---|:---|:---|
|`vector_col`|`Optional[str]`| Provide if you want to create index on a vector column. |`None`|
|`col_name`|`Optional[str]`| Provide if you want to create index on a non-vector column. |`None`|
|`metric`|`Optional[str]` |Provide the metric to use for vector index. choice of metrics: 'l2', 'dot', 'cosine'. |`l2`|
|`num_partitions`|`Optional[int]`|Number of partitions to use for the index.|`256`|
|`num_sub_vectors`|`Optional[int]` |Number of sub-vectors to use for the index.|`96`|
|`index_cache_size`|`Optional[int]` |Size of the index cache.|`None`|
|`name`|`Optional[str]` |Name of the table to create index on.|`None`|

For index creation make sure your table has enough data in it. An ANN index is ususally not needed for datasets ~100K vectors. For large-scale (>1M) or higher dimension vectors, it is beneficial to create an ANN index.

```python
# for creating vector index
vector_store.create_index(vector_col='vector', metric = 'cosine')

# for creating scalar index(for non-vector columns)
vector_store.create_index(col_name='text')

```

------

##### similarity_search()

This method performs similarity search based on **text query**.

| Name    | Type                 | Purpose | Default |
|---------|----------------------|---------|---------|
| `query` | `str`                |  A `str` representing the text query that you want to search for in the vector store.         | N/A     |
| `k`         | `Optional[int]`           | It specifies the number of documents to return.        | `None`    |
| `filter`    | `Optional[Dict[str, str]]`| It is used to filter the search results by specific metadata criteria.        | `None`    |
| `fts`   | `Optional[bool]`     |   It indicates whether to perform a full-text search (FTS).      | `False`   |
| `name`      | `Optional[str]`           | It is used for specifying the name of the table to query. If not provided, it uses the default table set during the initialization of the LanceDB instance.        | `None`    |
| `kwargs`    | `Any`                     | Other keyworded arguments provided by the user.        | N/A     |

Return documents most similar to the query **without relevance scores**.

```python
docs = docsearch.similarity_search(query)
print(docs[0].page_content)
```

------

##### similarity_search_by_vector()

The method returns documents that are most similar to the specified **embedding (query) vector**. 

| Name        | Type                      | Purpose | Default |
|-------------|---------------------------|---------|---------|
| `embedding` | `List[float]`             | The embedding vector you want to use to search for similar documents in the vector store.         | N/A     |
| `k`         | `Optional[int]`           | It specifies the number of documents to return.        | `None`    |
| `filter`    | `Optional[Dict[str, str]]`| It is used to filter the search results by specific metadata criteria.        | `None`    |
| `name`      | `Optional[str]`           | It is used for specifying the name of the table to query. If not provided, it uses the default table set during the initialization of the LanceDB instance.        | `None`    |
| `kwargs`    | `Any`                     | Other keyworded arguments provided by the user.        | N/A     |

**It does not provide relevance scores.**

```python
docs = docsearch.similarity_search_by_vector(query)
print(docs[0].page_content)
```

------

##### similarity_search_with_score()

Returns documents most similar to the **query string** along with their relevance scores.

| Name     | Type                      | Purpose | Default |
|----------|---------------------------|---------|---------|
| `query`  | `str`                     |A `str` representing the text query you want to search for in the vector store. This query will be converted into an embedding using the specified embedding function.         | N/A     |
| `k`      | `Optional[int]`           | It specifies the number of documents to return.  | `None`    |
| `filter` | `Optional[Dict[str, str]]`|  It is used to filter the search results by specific metadata criteria. This allows you to narrow down the search results based on certain metadata attributes associated with the documents.       | `None`    |
| `kwargs` | `Any`                     |  Other keyworded arguments provided by the user.       | N/A     |

It gets called by base class's `similarity_search_with_relevance_scores` which selects relevance score based on our `_select_relevance_score_fn`.

```python
docs = docsearch.similarity_search_with_relevance_scores(query)
print("relevance score - ", docs[0][1])
print("text- ", docs[0][0].page_content[:1000])
```

------

##### similarity_search_by_vector_with_relevance_scores()

Similarity search using **query vector**.

| Name        | Type                      | Purpose | Default |
|-------------|---------------------------|---------|---------|
| `embedding` | `List[float]`             | The embedding vector you want to use to search for similar documents in the vector store.        | N/A     |
| `k`         | `Optional[int]`           |   It specifies the number of documents to return.       | `None`    |
| `filter`    | `Optional[Dict[str, str]]`|  It is used to filter the search results by specific metadata criteria.       | `None`    |
| `name`      | `Optional[str]`           |   It is used for specifying the name of the table to query.       | `None`    |
| `kwargs`    | `Any`                     |   Other keyworded arguments provided by the user.      | N/A     |

The method returns documents most similar to the specified embedding (query) vector, along with their relevance scores.

```python
docs = docsearch.similarity_search_by_vector_with_relevance_scores(query_embedding)
print("relevance score - ", docs[0][1])
print("text- ", docs[0][0].page_content[:1000])
```

------

##### max_marginal_relevance_search()

This method returns docs selected using the maximal marginal relevance(MMR).
Maximal marginal relevance optimizes for similarity to query AND diversity among selected documents.

| Name | Type | Purpose | Default |
|---------------|-----------------|-----------|---------|
| `query`       | `str` | Text to look up documents similar to. | N/A     |
| `k`   | `Optional[int]` | Number of Documents to return.| `4`       |
| `fetch_k`| `Optional[int]`| Number of Documents to fetch to pass to MMR algorithm.| `None`    |
| `lambda_mult` | `float`                   | Number between 0 and 1 that determines the degree of diversity among the results with 0 corresponding to maximum diversity and 1 to minimum diversity. | `0.5`     |
| `filter`| `Optional[Dict[str, str]]`| Filter by metadata. | `None`    |
|`kwargs`| Other keyworded arguments provided by the user. |-|

Similarly, `max_marginal_relevance_search_by_vector()` function returns docs most similar to the embedding passed to the function using MMR. instead of a string query you need to pass the embedding to be searched for. 

```python
result = docsearch.max_marginal_relevance_search(
        query="text"
    )
result_texts = [doc.page_content for doc in result]
print(result_texts)

## search by vector :
result = docsearch.max_marginal_relevance_search_by_vector(
        embeddings.embed_query("text")
    )
result_texts = [doc.page_content for doc in result]
print(result_texts)
```

------

##### add_images()

This method ddds images by automatically creating their embeddings and adds them to the vectorstore.

| Name       | Type                          | Purpose                        | Default |
|------------|-------------------------------|--------------------------------|---------|
| `uris`     | `List[str]`                   | File path to the image         | N/A     |
| `metadatas`| `Optional[List[dict]]`        | Optional list of metadatas     | `None`    |
| `ids`      | `Optional[List[str]]`         | Optional list of IDs           | `None`    |

It returns list of IDs of the added images.

```python
vec_store.add_images(uris=image_uris) 
# here image_uris are local fs paths to the images.
```


