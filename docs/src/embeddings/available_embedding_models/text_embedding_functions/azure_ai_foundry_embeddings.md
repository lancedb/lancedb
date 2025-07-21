# Azure AI Foundry Embeddings

Azure AI Foundry provides embedding functions using the Azure AI API. These embeddings can be used for various tasks such as semantic search, clustering, and classification.

## Prerequisites

To use Azure AI Foundry embeddings, you need to set the following environment variables:

- `AZURE_AI_ENDPOINT`: The endpoint URL for the Azure AI service.
- `AZURE_AI_API_KEY`: The API key for the Azure AI service.

## Supported Parameters

| Parameter | Type   | Description |
|-----------|--------|-------------|
| `name`    | `str`  | The name of the model to use for embeddings. |
| `ndims`   | `int`  | The number of dimensions of the embeddings. This is required to create the vector column in LanceDB. |

## Usage Example

```python
import lancedb
import pandas as pd
from lancedb.pydantic import LanceModel, Vector
from lancedb.embeddings import get_registry

# Create an Azure AI embedding function
model = get_registry().get("azure-ai-text").create(name="embed-v-4-0", ndims=1536)

class TextModel(LanceModel):
    text: str = model.SourceField()
    vector: Vector(model.ndims()) = model.VectorField()

df = pd.DataFrame({"text": ["hello world", "goodbye world"]})
db = lancedb.connect("lance_example")
tbl = db.create_table("test", schema=TextModel, mode="overwrite")

tbl.add(df)
rs = tbl.search("hello").limit(1).to_pandas()
print(rs)
```

## Notes

- The embedding function batches requests to ensure no more than 96 texts are sent at once.
- The `ndims` parameter must be specified when creating the embedding function.