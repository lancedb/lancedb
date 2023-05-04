# Code Documentation Q&A Bot 2.0

## Simple Pandas 2.0 documentation Q&A answering bot using LangChain

<img id="splash" src="https://user-images.githubusercontent.com/917119/234710587-3b488467-e8a4-46ec-b377-fe24c17eecca.png"/>

To demonstrate using Lance, we’re going to build a simple Q&A answering bot using LangChain — an open-source framework that allows you to build composable LLM-based applications easily. We’ll use chat-langchain, a simple Q&A answering bot app as an example. Note: in this fork of chat-langchain, we’re also using a forked version of LangChain integration where we’ve built a Lance integration.

The first step is to generate embeddings. You could build a bot using your own data, like a wiki page or internal documentation. For this example, we’re going to use the Pandas API documentation. LangChain offers document loaders to read and pre-process many document types. Since the Pandas API is in HTML, reading the docs is straightforward:

```python
for p in Path("./pandas.documentation").rglob("*.html"):
   if p.is_dir():
     continue
   loader = UnstructuredHTMLLoader(p)
   raw_document = loader.load()
   docs = docs + raw_document
```

Once we have pre-processed our input documents, the next step is to generate the embeddings. For this, we’ll use LangChain’s OpenAI API wrapper. Note that you’ll need to sign up for the OpenAI API (this is a paid service). Here we’ll tokenize the documents and store them in a Lance dataset. Using Lance will persist your embeddings locally, you can read more about Lance’s data management features in its [documentation](https://eto-ai.github.io/lance/).

```python
text_splitter = RecursiveCharacterTextSplitter(
  chunk_size=1000,
  chunk_overlap=200
)
documents = text_splitter.split_documents(docs)
embeddings = OpenAIEmbeddings()
LanceDataset.from_documents(documents, embeddings, uri="pandas.lance")
```

Now we’ve got our vector store setup, we can boot up the chat app, which uses LangChain’s chain API to submit an input query to the vector store. Under the hood, this will generate embeddings for your query, perform similarity search using the vector store and generate the resulting text.

And presto! Your very own Pandas API helper bot, a handy little assistant to help you get up to speed with Pandas — here are some examples:

First let’s make sure we’re on the right document version for pandas:

Great, now we can ask some more specific questions:

So far so good!

# Integrating Lance into LangChain

LangChain has a vectorstore abstraction with multiple implementations. This is where we put Lance. In our own langchain fork, we added a lance_dataset.py as a new kind of vectorstore that is just a LanceDataset (pip install pylance). Once you get the embeddings, you can call lance’s vec_to_table() method to create a pyarrow Table from it:

```python
import lance
from lance.vector import vec_table
embeddings = embedding.embed_documents(texts)
tbl = vec_to_to_table(embeddings)
```

Writing the data is just:

```python
uri = "pandas_documentation.lance"
dataset = lance.write_dataset(tbl, uri)
```

If the dataset is small, Lance’s SIMD code for vector distances makes brute forcing faster than numpy. And if the dataset is large, you can create an ANN index in another 1 line of python code.

```python
dataset.create_index("vector", index_type="IVF_PQ",
                     num_partitions=256,  # ivf partitions
                     num_sub_vectors=num_sub_vectors)  # PQ subvectors
```

To make an ANN query to find 10 closest neighbors to the query_vector and also fetch the document and metadata, use the to_table function like this:

```python
tbl = self.dataset.to_table(columns=["document", "metadata"],
                            nearest={"column": "vector",
                                     "q": query_vector,
                                     "k": 10})
```

You now have a pyarrow Table that you can use for downstream re-ranking and filtering.

# Lance datasets are queryable

Lance datasets are Arrow compatible so you can directly query your Lance datasets using Pandas, DuckDB, Polars to make it super easy to do additional filtering, reranking, enrichment, and or debugging.

We store metadata in JSON alongside the vectors, but, if you wish, you could define the data model yourself.

For example if we want to check the number of vectors we’re storing, against what version of documentation in DuckDB, you can load the lance dataset in DuckDB and run SQL against it:

```sql
SELECT
 count(vector),
 json_extract(metadata, '$.version') as VERSION
FROM
  pandas_docs
GROUP BY
  VERSION
```

# Lance versions your data automatically

Oftentimes we have to regenerate our embeddings on a regular basis. This makes debugging a pain to have to track down the right version of your data and the right version of your index to diagnose any issues. With Lance you can create a new version of your dataset by specifying mode=”overwrite” when writing.

Let’s say we start with some toy data:

```python
>>> import pandas as pd
>>> import lance
>>> import numpy as np
>>> df = pd.DataFrame({"a": [5]})
>>> dataset = lance.write_dataset(df, "data.lance")
>>> dataset.to_table().to_pandas()
   a
0  5
```

We can create and persist a new version:

```python
>>> df = pd.DataFrame({"a": [50, 100]})
>>> dataset = lance.write_dataset(df, "data.lance", mode="overwrite")
>>> dataset.to_table().to_pandas()
     a
0   50
1  100
```

And you can time travel to a previous version by specifying the version number or timestamp when you create the dataset instance.

```python
>>> dataset.versions()
[{'version': 2, 'timestamp': datetime.datetime(2023, 2, 24, 11, 58, 20, 739008), 'metadata': {}}, 
 {'version': 1, 'timestamp': datetime.datetime(2023, 2, 24, 11, 56, 59, 690977), 'metadata': {}}]
>>> lance.dataset("data.lance", version=1).to_table().to_pandas()
   a
0  5
```

# Where Lance is headed

Lance’s random access performance makes it ideal to build search engines and high-performance data stores for deep learning. We’re actively working to make Lance support 1B+ scale vector datasets, partitioning and reindexing, and new index types. Lance is written in Rust and comes with a wrapper for python and an extension for duckdb.