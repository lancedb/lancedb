# OpenClip embeddings
We support CLIP model embeddings using the open source alternative, [open-clip](https://github.com/mlfoundations/open_clip) which supports various customizations. It is registered as `open-clip` and supports the following customizations:

| Parameter | Type | Default Value | Description |
|---|---|---|---|
| `name` | `str` | `"ViT-B-32"` | The name of the model. |
| `pretrained` | `str` | `"laion2b_s34b_b79k"` | The name of the pretrained model to load. |
| `device` | `str` | `"cpu"` | The device to run the model on. Can be `"cpu"` or `"gpu"`. |
| `batch_size` | `int` | `64` | The number of images to process in a batch. |
| `normalize` | `bool` | `True` | Whether to normalize the input images before feeding them to the model. |

This embedding function supports ingesting images as both bytes and urls. You can query them using both test and other images.

!!! info
    LanceDB supports ingesting images directly from accessible links.

```python
import lancedb
from lancedb.pydantic import LanceModel, Vector
from lancedb.embeddings import get_registry

db = lancedb.connect(tmp_path)
func = get_registry.get("open-clip").create()

class Images(LanceModel):
    label: str
    image_uri: str = func.SourceField() # image uri as the source
    image_bytes: bytes = func.SourceField() # image bytes as the source
    vector: Vector(func.ndims()) = func.VectorField() # vector column 
    vec_from_bytes: Vector(func.ndims()) = func.VectorField() # Another vector column 

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
image_bytes = [requests.get(uri).content for uri in uris]
table.add(
    pd.DataFrame({"label": labels, "image_uri": uris, "image_bytes": image_bytes})
)
```
Now we can search using text from both the default vector column and the custom vector column
```python

# text search
actual = table.search("man's best friend").limit(1).to_pydantic(Images)[0]
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
query_image = Image.open(io.BytesIO(image_bytes))
actual = table.search(query_image).limit(1).to_pydantic(Images)[0]
print(actual.label == "dog")

# image search using a custom vector column
other = (
    table.search(query_image, vector_column_name="vec_from_bytes")
    .limit(1)
    .to_pydantic(Images)[0]
)
print(actual.label)

```
