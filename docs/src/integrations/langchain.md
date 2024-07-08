# Langchain
![Illustration](../assets/langchain.png)

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
- `connection`: (Optional) `lancedb.db.LanceDBConnection` connection object to use.  If not provided, a new connection will be created.  
- `embedding`: Langchain embedding model.  
- `vector_key`: (Optional) Column name to use for vector's in the table. Defaults to `'vector'`.   
- `id_key`: (Optional) Column name to use for id's in the table. Defaults to `'id'`.  
- `text_key`: (Optional) Column name to use for text in the table. Defaults to `'text'`.  
- `table_name`: (Optional) Name of your table in the database. Defaults to `'vectorstore'`.  
- `api_key`: (Optional) API key to use for LanceDB cloud database. Defaults to `None`.  
- `region`: (Optional) Region to use for LanceDB cloud database. Only for LanceDB Cloud, defaults to `None`.  
- `mode`: (Optional) Mode to use for adding data to the table. Defaults to `'overwrite'`.  
- `reranker`: (Optional) The reranker to use for LanceDB.
- `relevance_score_fn`: (Optional[Callable[[float], float]]) Langchain relevance score function to be used. Defaults to `None`. 

```python
db_url = "db://lang_test" # url of db you created
api_key = "xxxxx" # your API key
region="us-east-1-dev"  # your selected region

vector_store = LanceDB(
    uri=db_url,
    api_key=api_key, #(dont include for local API)
    region=region, #(dont include for local API)
    embedding=embeddings,
    table_name='langchain_test' #Optional
    )
```

### Methods 

##### add_texts()
- `texts`: `Iterable` of strings to add to the vectorstore.
- `metadatas`: Optional `list[dict()]` of metadatas associated with the texts.
- `ids`: Optional `list` of ids to associate with the texts. 
- `kwargs`: `Any`

This method adds texts and stores respective embeddings automatically.

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
##### create_index() 
- `col_name`: `Optional[str] = None`
- `vector_col`: `Optional[str] = None`
- `num_partitions`: `Optional[int] = 256`
- `num_sub_vectors`: `Optional[int] = 96`
- `index_cache_size`: `Optional[int] = None`

This method creates an index for the vector store. For index creation make sure your table has enough data in it. An ANN index is ususally not needed for datasets ~100K vectors. For large-scale (>1M) or higher dimension vectors, it is beneficial to create an ANN index.

```python
# for creating vector index
vector_store.create_index(vector_col='vector', metric = 'cosine')

# for creating scalar index(for non-vector columns)
vector_store.create_index(col_name='text')

```

##### similarity_search()
- `query`: `str`
- `k`: `Optional[int] = None`
- `filter`: `Optional[Dict[str, str]] = None`
- `fts`: `Optional[bool] = False`
- `name`: `Optional[str] = None`
- `kwargs`: `Any`

Return documents most similar to the query without relevance scores

```python
docs = docsearch.similarity_search(query)
print(docs[0].page_content)
```

##### similarity_search_by_vector()
- `embedding`: `List[float]`
- `k`: `Optional[int] = None`
- `filter`: `Optional[Dict[str, str]] = None`
- `name`: `Optional[str] = None`
- `kwargs`: `Any`

Returns documents most similar to the query vector.

```python
docs = docsearch.similarity_search_by_vector(query)
print(docs[0].page_content)
```

##### similarity_search_with_score()
- `query`: `str`
- `k`: `Optional[int] = None`
- `filter`: `Optional[Dict[str, str]] = None`
- `kwargs`: `Any`

Returns documents most similar to the query string with relevance scores, gets called by base class's `similarity_search_with_relevance_scores` which selects relevance score based on our `_select_relevance_score_fn`.

```python
docs = docsearch.similarity_search_with_relevance_scores(query)
print("relevance score - ", docs[0][1])
print("text- ", docs[0][0].page_content[:1000])
```

##### similarity_search_by_vector_with_relevance_scores()
- `embedding`: `List[float]`
- `k`: `Optional[int] = None`
- `filter`: `Optional[Dict[str, str]] = None`
- `name`: `Optional[str] = None`
- `kwargs`: `Any`

Return documents most similar to the query vector with relevance scores.
Relevance score 

```python
docs = docsearch.similarity_search_by_vector_with_relevance_scores(query_embedding)
print("relevance score - ", docs[0][1])
print("text- ", docs[0][0].page_content[:1000])
```

##### max_marginal_relevance_search()
- `query`: `str`
- `k`: `Optional[int] = None`
- `fetch_k` : Number of Documents to fetch to pass to MMR algorithm, `Optional[int] = None`
- `lambda_mult`: Number between 0 and 1 that determines the degree
                        of diversity among the results with 0 corresponding
                        to maximum diversity and 1 to minimum diversity.
                        Defaults to 0.5. `float = 0.5`
- `filter`: `Optional[Dict[str, str]] = None`
- `kwargs`: `Any`

Returns docs selected using the maximal marginal relevance(MMR).
Maximal marginal relevance optimizes for similarity to query AND diversity among selected documents.

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

##### add_images()
- `uris` : File path to the image. `List[str]`.
- `metadatas` : Optional list of metadatas. `(Optional[List[dict]], optional)`
- `ids` : Optional list of IDs. `(Optional[List[str]], optional)`

Adds images by automatically creating their embeddings and adds them to the vectorstore.

```python
vec_store.add_images(uris=image_uris) 
# here image_uris are local fs paths to the images.
```


