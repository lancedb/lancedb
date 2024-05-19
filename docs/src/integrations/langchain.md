# Langchain
![Illustration](../assets/langchain.png)

## Quick Start
You can load your document data using langchain's loaders, for this example we are using `TextLoader` and `OpenAIEmbeddings` as the embedding model.
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
To add texts and store respective embeddings automatically:   
##### add_texts()
- `texts`: `Iterable` of strings to add to the vectorstore.
- `metadatas`: Optional `list[dict()]` of metadatas associated with the texts.
- `ids`: Optional `list` of ids to associate with the texts. 


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
For index creation make sure your table has enough data in it. An ANN index is ususally not needed for datasets ~100K vectors. For large-scale (>1M) or higher dimension vectors, it is beneficial to create an ANN index.
##### create_index() 
- `col_name`: `Optional[str] = None`
- `vector_col`: `Optional[str] = None`
- `num_partitions`: `Optional[int] = 256`
- `num_sub_vectors`: `Optional[int] = 96`
- `index_cache_size`: `Optional[int] = None`

```python
# for creating vector index
vector_store.create_index(vector_col='vector', metric = 'cosine')

# for creating scalar index(for non-vector columns)
vector_store.create_index(col_name='text')

```