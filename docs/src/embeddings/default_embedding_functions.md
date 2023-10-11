There are various Embedding functions available out of the box with lancedb. We're working on supporting other popular embedding APIs.

## Text Embedding Functions
Here are the text embedding functions registered by default

### Sentence Transformers
Here are the parameters that you can set when registering a `sentence-transformers` object, and their default values:

| Parameter | Type | Default Value | Description |
|---|---|---|---|
| `name` | `str` | `"all-MiniLM-L6-v2"` | The name of the model. |
| `device` | `str` | `"cpu"` | The device to run the model on. Can be `"cpu"` or `"gpu"`. |
| `normalize` | `bool` | `True` | Whether to normalize the input text before feeding it to the model. |


```python
db = lancedb.connect("/tmp/db")
registry = EmbeddingFunctionRegistry.get_instance()
func = registry.get("sentence-transformers").create(device="cpu")

class Words(LanceModel):
    text: str = func.SourceField()
    vector: Vector(func.ndims()) = func.VectorField()

table = db.create_table("words", schema=Words)
table.add(
    pd.DataFrame(
        {
            "text": [
                "hello world",
                "goodbye world",
            ]
        }
    )
)

query = "greetings"
actual = table.search(query).limit(1).to_pydantic(Words)[0]
print(actual.text)
```

### OpenAIEmbeddings
LanceDB has OpenAI embeddings function in the registry by default. It is registered as `openai` and here are the parameters that you can customize when creating the instances

| Parameter | Type | Default Value | Description |
|---|---|---|---|
| `name` | `str` | `"text-embedding-ada-002"` | The name of the model. |



```python
db = lancedb.connect("/tmp/db")
registry = EmbeddingFunctionRegistry.get_instance()
func = registry.get("openai").create()

class Words(LanceModel):
    text: str = func.SourceField()
    vector: Vector(func.ndims()) = func.VectorField()

table = db.create_table("words", schema=Words)
table.add(
    pd.DataFrame(
        {
            "text": [
                "hello world",
                "goodbye world",
                ]
        }))

query = "greetings"
actual = table.search(query).limit(1).to_pydantic(Words)[0]
print(actual.text)
```

## Multi-modal embedding functions
Multi-modal embedding functions allow you query your table using both images and text.

### OpenClipEmbeddings
We support CLIP model embeddings using the open souce alternbative, open-clip which support various customizations. It is registered as `open-clip` and supports following customizations.


| Parameter | Type | Default Value | Description |
|---|---|---|---|
| `name` | `str` | `"ViT-B-32"` | The name of the model. |
| `pretrained` | `str` | `"laion2b_s34b_b79k"` | The name of the pretrained model to load. |
| `device` | `str` | `"cpu"` | The device to run the model on. Can be `"cpu"` or `"gpu"`. |
| `batch_size` | `int` | `64` | The number of images to process in a batch. |
| `normalize` | `bool` | `True` | Whether to normalize the input images before feeding them to the model. |


This embedding function supports ingesting images as both bytes and urls. You can query them using both test and other images.

NOTE:
LanceDB supports ingesting images directly from accessible links.


```python

db = lancedb.connect(tmp_path)
registry = EmbeddingFunctionRegistry.get_instance()
func = registry.get("open-clip").create()

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

If you have any questions about the embeddings API, supported models, or see a relevant model missing, please raise an issue.