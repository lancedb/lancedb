# VoyageAI Embeddings : Multimodal

VoyageAI embeddings can also be used to embed both text and image data, only some of the models support image data and you can check the list
under [https://docs.voyageai.com/docs/multimodal-embeddings](https://docs.voyageai.com/docs/multimodal-embeddings)

Supported multimodal models:

- `voyage-multimodal-3` - 1024 dimensions (text + images)
- `voyage-multimodal-3.5` - Flexible dimensions (256, 512, 1024 default, 2048). Supports text, images, and video.

### Video Support (voyage-multimodal-3.5)

The `voyage-multimodal-3.5` model supports video input through:
- Video URLs (`.mp4`, `.webm`, `.mov`, `.avi`, `.mkv`, `.m4v`, `.gif`)
- Video file paths

Constraints: Max 20MB video size.

Supported parameters (to be passed in `create` method) are:

| Parameter | Type | Default Value           | Description                               |
|---|---|-------------------------|-------------------------------------------|
| `name` | `str` | `"voyage-multimodal-3"` | The model ID of the VoyageAI model to use |
| `output_dimension` | `int` | `None` | Output dimension for voyage-multimodal-3.5. Valid: 256, 512, 1024, 2048 |

Usage Example:

```python
import base64
import os
from io import BytesIO

import requests
import lancedb
from lancedb.pydantic import LanceModel, Vector
from lancedb.embeddings import get_registry
import pandas as pd

os.environ['VOYAGE_API_KEY'] = 'YOUR_VOYAGE_API_KEY'

db = lancedb.connect(".lancedb")
func = get_registry().get("voyageai").create(name="voyage-multimodal-3")


def image_to_base64(image_bytes: bytes):
    buffered = BytesIO(image_bytes)
    img_str = base64.b64encode(buffered.getvalue())
    return img_str.decode("utf-8")


class Images(LanceModel):
    label: str
    image_uri: str = func.SourceField()  # image uri as the source
    image_bytes: str = func.SourceField()  # image bytes base64 encoded as the source
    vector: Vector(func.ndims()) = func.VectorField()  # vector column
    vec_from_bytes: Vector(func.ndims()) = func.VectorField()  # Another vector column


if "images" in db.table_names():
    db.drop_table("images")
table = db.create_table("images", schema=Images)
labels = ["cat", "cat", "dog", "dog", "horse", "horse"]
uris = [
    "http://farm1.staticflickr.com/53/167798175_7c7845bbbd_z.jpg",
    "http://farm1.staticflickr.com/134/332220238_da527d8140_z.jpg",
    "http://farm9.staticflickr.com/8387/8602747737_2e5c2a45d4_z.jpg",
    "http://farm5.staticflickr.com/4092/5017326486_1f46057f5f_z.jpg",
    "http://farm9.staticflickr.com/8216/8434969557_d37882c42d_z.jpg",
    "http://farm6.staticflickr.com/5142/5835678453_4f3a4edb45_z.jpg",
]
# get each uri as bytes
images_bytes = [image_to_base64(requests.get(uri).content) for uri in uris]
table.add(
    pd.DataFrame({"label": labels, "image_uri": uris, "image_bytes": images_bytes})
)
```
Now we can search using text from both the default vector column and the custom vector column
```python

# text search
actual = table.search("man's best friend", "vec_from_bytes").limit(1).to_pydantic(Images)[0]
print(actual.label) # prints "dog"

frombytes = (
    table.search("man's best friend", vector_column_name="vec_from_bytes")
    .limit(1)
    .to_pydantic(Images)[0]
)
print(frombytes.label)

```

Because we're using a multi-modal embedding function, we can also search using images

```python
# image search
query_image_uri = "http://farm1.staticflickr.com/200/467715466_ed4a31801f_z.jpg"
image_bytes = requests.get(query_image_uri).content
query_image = Image.open(BytesIO(image_bytes))
actual = table.search(query_image, "vec_from_bytes").limit(1).to_pydantic(Images)[0]
print(actual.label == "dog")

# image search using a custom vector column
other = (
    table.search(query_image, vector_column_name="vec_from_bytes")
    .limit(1)
    .to_pydantic(Images)[0]
)
print(actual.label)

```
