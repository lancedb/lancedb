# IBM watsonx.ai Embeddings

Generate text embeddings using IBM's watsonx.ai platform.

## Supported Models

You can find a list of supported models at [IBM watsonx.ai Documentation](https://dataplatform.cloud.ibm.com/docs/content/wsj/analyze-data/fm-models-embed.html?context=wx). The currently supported model names are:

- `ibm/slate-125m-english-rtrvr`
- `ibm/slate-30m-english-rtrvr`
- `sentence-transformers/all-minilm-l12-v2`
- `intfloat/multilingual-e5-large`

## Parameters

The following parameters can be passed to the `create` method:

| Parameter  | Type     | Default Value                    | Description                                               |
|------------|----------|----------------------------------|-----------------------------------------------------------|
| name       | str      | "ibm/slate-125m-english-rtrvr"   | The model ID of the watsonx.ai model to use               |
| api_key    | str      | None                             | Optional IBM Cloud API key (or set `WATSONX_API_KEY`)     |
| project_id | str      | None                             | Optional watsonx project ID (or set `WATSONX_PROJECT_ID`) |
| url        | str      | None                             | Optional custom URL for the watsonx.ai instance           |
| params     | dict     | None                             | Optional additional parameters for the embedding model    |

## Usage Example

First, the watsonx.ai library is an optional dependency, so must be installed seperately:

```
pip install ibm-watsonx-ai
```

Optionally set environment variables (if not passing credentials to `create` directly):

```sh
export WATSONX_API_KEY="YOUR_WATSONX_API_KEY"
export WATSONX_PROJECT_ID="YOUR_WATSONX_PROJECT_ID"
```

```python
import os
import lancedb
from lancedb.pydantic import LanceModel, Vector
from lancedb.embeddings import EmbeddingFunctionRegistry

watsonx_embed = EmbeddingFunctionRegistry
  .get_instance()
  .get("watsonx")
  .create(
    name="ibm/slate-125m-english-rtrvr",
    # Uncomment and set these if not using environment variables
    # api_key="your_api_key_here",
    # project_id="your_project_id_here",
    # url="your_watsonx_url_here",
    # params={...},
  )

class TextModel(LanceModel):
    text: str = watsonx_embed.SourceField()
    vector: Vector(watsonx_embed.ndims()) = watsonx_embed.VectorField()

data = [
    {"text": "hello world"},
    {"text": "goodbye world"},
]

db = lancedb.connect("~/.lancedb")
tbl = db.create_table("watsonx_test", schema=TextModel, mode="overwrite")

tbl.add(data)

rs = tbl.search("hello").limit(1).to_pandas()
print(rs)
```