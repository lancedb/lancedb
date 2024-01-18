In this workflow, you define your own embedding function and pass it as a callable to LanceDB, invoking it in your code to generate the embeddings. Let's look at some examples.

### Hugging Face

!!! note
    Currently, the Hugging Face method is only supported in the Python SDK.

=== "Python"
    The most popular open source option is to use the [sentence-transformers](https://www.sbert.net/) 
    library, which can be installed via pip.

    ```bash
    pip install sentence-transformers
    ```

    The example below shows how to use the `paraphrase-albert-small-v2` model to generate embeddings 
    for a given document.

    ```python
    from sentence_transformers import SentenceTransformer

    name="paraphrase-albert-small-v2"
    model = SentenceTransformer(name)

    # used for both training and querying
    def embed_func(batch):
        return [model.encode(sentence) for sentence in batch]
    ```

### OpenAI

Another popular alternative is to use an external API like OpenAI's [embeddings API](https://platform.openai.com/docs/guides/embeddings/what-are-embeddings).

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

=== "JavaScript"
      ```javascript
        const lancedb = require("vectordb");

        // You need to provide an OpenAI API key
        const apiKey = "sk-..."
        // The embedding function will create embeddings for the 'text' column
        const embedding = new lancedb.OpenAIEmbeddingFunction('text', apiKey)
      ```

## Applying an embedding function to data

=== "Python"
    Using an embedding function, you can apply it to raw data
    to generate embeddings for each record.

    Say you have a pandas DataFrame with a `text` column that you want embedded,
    you can use the `with_embeddings` function to generate embeddings and add them to 
    an existing table.

    ```python
     import pandas as pd
     from lancedb.embeddings import with_embeddings

     df = pd.DataFrame(
        [
            {"text": "pepperoni"},
            {"text": "pineapple"}
        ]
    )
     data = with_embeddings(embed_func, df)

     # The output is used to create / append to a table
     # db.create_table("my_table", data=data)
    ```

    If your data is in a different column, you can specify the `column` kwarg to `with_embeddings`.

    By default, LanceDB calls the function with batches of 1000 rows. This can be configured
    using the `batch_size` parameter to `with_embeddings`.

    LanceDB automatically wraps the function with retry and rate-limit logic to ensure the OpenAI
    API call is reliable.

=== "JavaScript"
    Using an embedding function, you can apply it to raw data
    to generate embeddings for each record.

    Simply pass the embedding function created above and LanceDB will use it to generate
    embeddings for your data.

    ```javascript
    const db = await lancedb.connect("data/sample-lancedb");
    const data = [
    { text: "pepperoni"},
    { text: "pineapple"}
    ]

    const table = await db.createTable("vectors", data, embedding)
    ```

## Querying using an embedding function

!!! warning
    At query time, you **must** use the same embedding function you used to vectorize your data.
    If you use a different embedding function, the embeddings will not reside in the same vector
    space and the results will be nonsensical.

=== "Python"
     ```python
     query = "What's the best pizza topping?"
     query_vector = embed_func([query])[0]
     results = (
        tbl.search(query_vector)
        .limit(10)
        .to_pandas()
     )
     ```

     The above snippet returns a pandas DataFrame with the 10 closest vectors to the query.

=== "JavaScript"
     ```javascript
      const results = await table
        .search("What's the best pizza topping?")
        .limit(10)
        .execute()
     ```

     The above snippet returns an array of records with the top 10 nearest neighbors to the query.