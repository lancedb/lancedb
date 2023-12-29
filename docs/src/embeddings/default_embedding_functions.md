There are various Embedding functions available out of the box with LanceDB. We're working on supporting other popular embedding APIs.

## Text Embedding Functions
Here are the text embedding functions registered by default.
Embedding functions have an inbuilt rate limit handler wrapper for source and query embedding function calls that retry with exponential standoff. 
Each `EmbeddingFunction` implementation automatically takes `max_retries` as an argument which has the default value of 7.

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
    [
        {"text": "hello world"}
        {"text": "goodbye world"}
    ]
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
    [
        {"text": "hello world"}
        {"text": "goodbye world"}
    ]
    )

query = "greetings"
actual = table.search(query).limit(1).to_pydantic(Words)[0]
print(actual.text)
```

### Instructor Embeddings
Instructor is an instruction-finetuned text embedding model that can generate text embeddings tailored to any task (e.g. classification, retrieval, clustering, text evaluation, etc.) and domains (e.g. science, finance, etc.) by simply providing the task instruction, without any finetuning.

If you want to calculate customized embeddings for specific sentences, you may follow the unified template to write instructions:

                          Represent the `domain` `text_type` for `task_objective`:

* `domain` is optional, and it specifies the domain of the text, e.g. science, finance, medicine, etc.
* `text_type` is required, and it specifies the encoding unit, e.g. sentence, document, paragraph, etc.
* `task_objective` is optional, and it specifies the objective of embedding, e.g. retrieve a document, classify the sentence, etc.

More information about the model can be found here - https://github.com/xlang-ai/instructor-embedding

| Argument | Type | Default | Description |
|---|---|---|---|
| `name` | `str` | "hkunlp/instructor-base" | The name of the model to use |
| `batch_size` | `int` | `32` | The batch size to use when generating embeddings |
| `device` | `str` | `"cpu"` | The device to use when generating embeddings |
| `show_progress_bar` | `bool` | `True` | Whether to show a progress bar when generating embeddings |
| `normalize_embeddings` | `bool` | `True` | Whether to normalize the embeddings |
| `quantize` | `bool` | `False` | Whether to quantize the model |
| `source_instruction` | `str` | `"represent the docuement for retreival"` | The instruction for the source column |
| `query_instruction` | `str` | `"represent the document for retreiving the most similar documents"` | The instruction for the query |



```python
import lancedb
from lancedb.pydantic import LanceModel, Vector
from lancedb.embeddings import get_registry, InstuctorEmbeddingFunction

instructor = get_registry().get("instructor").create(
                            source_instruction="represent the docuement for retreival",
                            query_instruction="represent the document for retreiving the most similar documents"
                            )

class Schema(LanceModel):
    vector: Vector(instructor.ndims()) = instructor.VectorField()
    text: str = instructor.SourceField()

db = lancedb.connect("~/.lancedb")
tbl = db.create_table("test", schema=Schema, mode="overwrite")

texts = [{"text": "Capitalism has been dominant in the Western world since the end of feudalism, but most feel[who?] that..."},
        {"text": "The disparate impact theory is especially controversial under the Fair Housing Act because the Act..."},
        {"text": "Disparate impact in United States labor law refers to practices in employment, housing, and other areas that.."}]

tbl.add(texts)
```

## Multi-modal embedding functions
Multi-modal embedding functions allow you to query your table using both images and text.

### OpenClipEmbeddings
We support CLIP model embeddings using the open source alternative, open-clip which supports various customizations. It is registered as `open-clip` and supports the following customizations:


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
    [{"label": labels, "image_uri": uris, "image_bytes": image_bytes}]
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
