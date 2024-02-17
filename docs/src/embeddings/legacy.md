The legacy `with_embeddings` API is for Python only and is deprecated.

### Hugging Face

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

```python
import openai
import os

# Configuring the environment variable OPENAI_API_KEY
if "OPENAI_API_KEY" not in os.environ:
# OR set the key here as a variable
openai.api_key = "sk-..."

client = openai.OpenAI()

def embed_func(c):    
    rs = client.embeddings.create(input=c, model="text-embedding-ada-002")
    return [record.embedding for record in rs["data"]]
```


## Applying an embedding function to data

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
    tbl = db.create_table("my_table", data=data)
```

If your data is in a different column, you can specify the `column` kwarg to `with_embeddings`.

By default, LanceDB calls the function with batches of 1000 rows. This can be configured
using the `batch_size` parameter to `with_embeddings`.

LanceDB automatically wraps the function with retry and rate-limit logic to ensure the OpenAI
API call is reliable.

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
