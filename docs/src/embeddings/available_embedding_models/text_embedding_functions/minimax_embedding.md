# MiniMax Embeddings

MiniMax provides high-quality text embedding models via their cloud API.

Using the MiniMax embedding API requires the `requests` package, which can be installed using `pip install requests`.
You also need to set the `MINIMAX_API_KEY` environment variable to use the MiniMax API.

Supported models:

- **embo-01** (1536 dims, general-purpose text embedding)

MiniMax embeddings support two embedding types:
- **db**: Optimized for storage/indexing (used for source embeddings)
- **query**: Optimized for search queries (used for query embeddings)

Supported parameters (to be passed in `create` method):

| Parameter | Type | Default Value | Description |
|---|---|--------|---------|
| `name` | `str` | `"embo-01"` | The model ID of the embedding model to use. |
| `api_key` | `str` | `None` | The API key. If not provided, the `MINIMAX_API_KEY` environment variable will be used. |


Usage Example:

```python
import lancedb
from lancedb.pydantic import LanceModel, Vector
from lancedb.embeddings import get_registry

minimax = get_registry().get("minimax").create()

class TextModel(LanceModel):
    text: str = minimax.SourceField()
    vector: Vector(minimax.ndims()) = minimax.VectorField()

data = [{"text": "hello world"}, {"text": "goodbye world"}]

db = lancedb.connect("~/.lancedb")
tbl = db.create_table("minimax_test", schema=TextModel, mode="overwrite")

tbl.add(data)

# Search
results = tbl.search("greetings").limit(1).to_list()
```
