# Embedding Functions

Embeddings are high dimensional floating-point vector representations of your data or query.
Anything can be embedded using some embedding model or function.
For a given embedding function, the output will always have the same number of dimensions.

## Creating an embedding function

Any function that takes as input a batch (list) of data and outputs a batch (list) of embeddings
can be used by LanceDB as an embedding function. The input and output batch sizes should be the same.

### HuggingFace example

One popular free option would be to use the [sentence-transformers](https://www.sbert.net/) library from HuggingFace.
You can install this using pip: `pip install sentence-transformers`.

```python
from sentence_transformers import SentenceTransformer

name="paraphrase-albert-small-v2"
model = SentenceTransformer(name)

# used for both training and querying
def embed_func(batch):
    return [model.encode(sentence) for sentence in batch]
```

Please note that currently HuggingFace is only supported in the Python SDK.

### OpenAI example

You can also use an external API like OpenAI to generate embeddings

=== "Python"
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

=== "Javascript"
      ```javascript
        const lancedb = require("vectordb");

        // You need to provide an OpenAI API key
        const apiKey = "sk-..."
        // The embedding function will create embeddings for the 'text' column
        const embedding = new lancedb.OpenAIEmbeddingFunction('text', apiKey)
      ```

## Applying an embedding function

=== "Python"
    Using an embedding function, you can apply it to raw data
    to generate embeddings for each row.

    Say if you have a pandas DataFrame with a `text` column that you want to be embedded,
    you can use the [with_embeddings](https://lancedb.github.io/lancedb/python/python/#lancedb.embeddings.with_embeddings)
    function to generate embeddings and add create a combined pyarrow table:


     ```python
     import pandas as pd
     from lancedb.embeddings import with_embeddings

     df = pd.DataFrame([{"text": "pepperoni"},
                        {"text": "pineapple"}])
     data = with_embeddings(embed_func, df)

     # The output is used to create / append to a table
     # db.create_table("my_table", data=data)
     ```

     If your data is in a different column, you can specify the `column` kwarg to `with_embeddings`.

     By default, LanceDB calls the function with batches of 1000 rows. This can be configured
     using the `batch_size` parameter to `with_embeddings`.

     LanceDB automatically wraps the function with retry and rate-limit logic to ensure the OpenAI
     API call is reliable.

=== "Javascript"
     Using an embedding function, you can apply it to raw data
     to generate embeddings for each row.

     You can just pass the embedding function created previously and LanceDB will automatically generate
     embededings for your data.

      ```javascript
      const db = await lancedb.connect("data/sample-lancedb");
      const data = [
        { text: 'pepperoni'  },
        { text: 'pineapple' }
      ]

      const table = await db.createTable('vectors', data, embedding)
      ```


## Searching with an embedding function

At inference time, you also need the same embedding function to embed your query text.
It's important that you use the same model / function otherwise the embedding vectors don't
belong in the same latent space and your results will be nonsensical.

=== "Python"
     ```python
     query = "What's the best pizza topping?"
     query_vector = embed_func([query])[0]
     tbl.search(query_vector).limit(10).to_df()
     ```

     The above snippet returns a pandas DataFrame with the 10 closest vectors to the query.

=== "Javascript"
     ```javascript
      const results = await table
        .search("What's the best pizza topping?")
        .limit(10)
        .execute()
     ```

     The above snippet returns an array of records with the 10 closest vectors to the query.


## Roadmap

In the near future, we'll be integrating the embedding functions deeper into LanceDB<br/>.
The goal is that you just have to configure the function once when you create the table,
and then you'll never have to deal with embeddings / vectors after that unless you want to.
We'll also integrate more popular models and APIs.
