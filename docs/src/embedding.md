# Embedding Functions

Embeddings are high dimensional floating-point vector representations of your data or query.
Anything can be embedded using some embedding model or function.
For a given embedding function, the output will always have the same number of dimensions.

## Creating an embedding function

Any function that takes as input a batch (list) of data and outputs a batch (list) of embeddings
can be used by LanceDB as an embedding function.

### HuggingFace example

One popular free option would be to use the sentence-transformers library from HuggingFace.

```python
from sentence_transformers import SentenceTransformer

name="paraphrase-albert-small-v2"
model = SentenceTransformer(name)

# used for both training and querying
def embed_func(batch):
    return [model.encode(sentence) for sentence in batch]
```

### OpenAI example

You can also use an external API like OpenAI to generate embeddings

```python
import openai
import os

# Configuring the environment variable OPENAI_API_KEY
if "OPENAI_API_KEY" not in os.environ:
    # OR set the key here as a variable
    openai.api_key = "sk-..."

# verify that the API key is working
assert len(openai.Model.list()["data"]) > 0

def embed_func(c):
    rs = openai.Embedding.create(input=c, engine="text-embedding-ada-002")
    return [record["embedding"] for record in rs["data"]]
```

## Applying an embedding function

Using an embedding function, you can apply it to raw data
to generate embeddings for each row.

Say if you have a pandas DataFrame with a `text` column that you want to be embedded,
you can use the following code to generate embeddings and add create a combined
pyarrow table:

```python
from lancedb.embeddings import with_embeddings

data = with_embeddings(embed_func, df)

# The output is used to create / append to a table
# db.create_table("my_table", data=data)
```

By default, LanceDB calls the function with batches of 1000 rows. This can be configured
using the `batch_size` parameter to `with_embeddings`.

LanceDB automatically wraps the function with retry and rate-limit logic to ensure the OpenAI
API call is reliable.

## Searching with an embedding function

At inference time, you also need the same embedding function to embed your query text.
It's important that you use the same model / function otherwise the embedding vectors don't
belong in the same latent space and your results will be nonsensical.

```python
query_vector = embed_func([query])[0]
tbl.search(query_vector).limit(10).to_df()
```

The above snippet returns a pandas DataFrame with the 10 closest vectors to the query.

## Roadmap

In the near future, we'll be integrating the embedding functions deeper into LanceDB<br/>.
The goal is that you just have to configure the function once when you create the table,
and then you'll never have to deal with embeddings / vectors after that unless you want to.
We'll also integrate more popular models and APIs.
