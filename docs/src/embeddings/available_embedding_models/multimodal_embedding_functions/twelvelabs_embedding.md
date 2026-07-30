# TwelveLabs Embeddings : Multimodal

[TwelveLabs](https://twelvelabs.io) Marengo is a multimodal model that maps
text, images, audio and video into a single shared embedding space. That means
you can store image (or other media) embeddings as the source column and query
them with plain text, or vice-versa.

Set the `TWELVELABS_API_KEY` environment variable with your API key. You can
grab a free key at [https://twelvelabs.io](https://twelvelabs.io) — there's a
generous free tier.

Supported models:

- `marengo3.0` - 512 dimensions (default)
- `Marengo-retrieval-2.7` - 1024 dimensions

Supported parameters (to be passed in the `create` method):

| Parameter | Type | Default Value | Description |
|---|---|---|---|
| `name` | `str` | `"marengo3.0"` | The Marengo model to use |

Source and query inputs may be text (`str`), an image URL (`str`), a local
image path (`pathlib.Path`), raw image `bytes`, or a `PIL.Image.Image`.

Usage Example:

```python
import os

import lancedb
from lancedb.pydantic import LanceModel, Vector
from lancedb.embeddings import get_registry

os.environ["TWELVELABS_API_KEY"] = "YOUR_TWELVELABS_API_KEY"

db = lancedb.connect(".lancedb")
func = get_registry().get("twelvelabs").create(name="marengo3.0")


class Images(LanceModel):
    label: str
    image_uri: str = func.SourceField()  # image uri as the source
    vector: Vector(func.ndims()) = func.VectorField()  # vector column


table = db.create_table("images", schema=Images, mode="overwrite")
table.add(
    [
        {"label": "cat", "image_uri": "http://farm1.staticflickr.com/53/167798175_7c7845bbbd_z.jpg"},
        {"label": "dog", "image_uri": "http://farm9.staticflickr.com/8387/8602747737_2e5c2a45d4_z.jpg"},
    ]
)
```

Because Marengo is multimodal, we can search the image column with a text query:

```python
result = table.search("man's best friend").limit(1).to_pydantic(Images)[0]
print(result.label)  # prints "dog"
```
