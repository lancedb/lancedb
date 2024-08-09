Due to the nature of vector embeddings, they can be used to represent any kind of data, from text to images to audio.
This makes them a very powerful tool for machine learning practitioners.
However, there's no one-size-fits-all solution for generating embeddings - there are many different libraries and APIs
(both commercial and open source) that can be used to generate embeddings from structured/unstructured data.

LanceDB supports 3 methods of working with embeddings.

1. You can manually generate embeddings for the data and queries. This is done outside of LanceDB.
2. You can use the built-in [embedding functions](./embedding_functions.md) to embed the data and queries in the background.
3. You can define your own [custom embedding function](./custom_embedding_function.md)
   that extends the default embedding functions.

For python users, there is also a legacy [with_embeddings API](./legacy.md).
It is retained for compatibility and will be removed in a future version.

## Quickstart

To get started with embeddings, you can use the built-in embedding functions.

### OpenAI Embedding function

LanceDB registers the OpenAI embeddings function in the registry as `openai`. You can pass any supported model name to the `create`. By default it uses `"text-embedding-ada-002"`.

=== "Python"

    ```python
    import lancedb
    from lancedb.pydantic import LanceModel, Vector
    from lancedb.embeddings import get_registry

    db = lancedb.connect("/tmp/db")
    func = get_registry().get("openai").create(name="text-embedding-ada-002")

    class Words(LanceModel):
        text: str = func.SourceField()
        vector: Vector(func.ndims()) = func.VectorField()

    table = db.create_table("words", schema=Words, mode="overwrite")
    table.add(
        [
            {"text": "hello world"},
            {"text": "goodbye world"}
        ]
        )

    query = "greetings"
    actual = table.search(query).limit(1).to_pydantic(Words)[0]
    print(actual.text)
    ```

=== "TypeScript"

    ```typescript
    --8<--- "nodejs/examples/embedding.ts:imports"
    --8<--- "nodejs/examples/embedding.ts:openai_embeddings"
    ```

=== "Rust"

    ```rust
    --8<--- "rust/lancedb/examples/openai.rs:imports"
    --8<--- "rust/lancedb/examples/openai.rs:openai_embeddings"
    ```

### Sentence Transformers Embedding function
LanceDB registers the Sentence Transformers embeddings function in the registry as `sentence-transformers`. You can pass any supported model name to the `create`. By default it uses `"sentence-transformers/paraphrase-MiniLM-L6-v2"`.

=== "Python"
    ```python
    import lancedb
    from lancedb.pydantic import LanceModel, Vector
    from lancedb.embeddings import get_registry

    db = lancedb.connect("/tmp/db")
    model = get_registry().get("sentence-transformers").create(name="BAAI/bge-small-en-v1.5", device="cpu")

    class Words(LanceModel):
        text: str = model.SourceField()
        vector: Vector(model.ndims()) = model.VectorField()

    table = db.create_table("words", schema=Words)
    table.add(
        [
            {"text": "hello world"},
            {"text": "goodbye world"}
        ]
    )

    query = "greetings"
    actual = table.search(query).limit(1).to_pydantic(Words)[0]
    print(actual.text)
    ```

=== "TypeScript"

    Coming Soon!

=== "Rust"

    Coming Soon!

### Embedding function with LanceDB cloud
Embedding functions are now supported on LanceDB cloud. The embeddings will be generated on the source device and sent to the cloud. This means that the source device must have the necessary resources to generate the embeddings. Here's an example using the OpenAI embedding function:

```python
import os
import lancedb
from lancedb.pydantic import LanceModel, Vector
from lancedb.embeddings import get_registry
os.environ['OPENAI_API_KEY'] = "..."

db = lancedb.connect(
  uri="db://....",
  api_key="sk_...",
  region="us-east-1"
)
func = get_registry().get("openai").create()

class Words(LanceModel):
    text: str = func.SourceField()
    vector: Vector(func.ndims()) = func.VectorField()

table = db.create_table("words", schema=Words)
table.add(
    [
        {"text": "hello world"},
        {"text": "goodbye world"}
    ]
    )

query = "greetings"
actual = table.search(query).limit(1).to_pydantic(Words)[0]
print(actual.text)
```
