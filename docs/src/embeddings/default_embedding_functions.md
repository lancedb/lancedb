There are various embedding functions available out of the box with LanceDB to manage your embeddings implicitly. We're actively working on adding other popular embedding APIs and models.

## Text embedding functions
Contains the text embedding functions registered by default.

* Embedding functions have an inbuilt rate limit handler wrapper for source and query embedding function calls that retry with exponential backoff. 
* Each `EmbeddingFunction` implementation automatically takes `max_retries` as an argument which has the default value of 7.

### Sentence transformers
Allows you to set parameters when registering a `sentence-transformers` object.

!!! info
    Sentence transformer embeddings are normalized by default. It is recommended to use normalized embeddings for similarity search.

| Parameter | Type | Default Value | Description |
|---|---|---|---|
| `name` | `str` | `all-MiniLM-L6-v2` | The name of the model |
| `device` | `str` | `cpu` | The device to run the model on (can be `cpu` or `gpu`) |
| `normalize` | `bool` | `True` | Whether to normalize the input text before feeding it to the model |


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

### OpenAI embeddings
LanceDB registers the OpenAI embeddings function in the registry by default, as `openai`. Below are the parameters that you can customize when creating the instances:

| Parameter | Type | Default Value | Description |
|---|---|---|---|
| `name` | `str` | `"text-embedding-ada-002"` | The name of the model. |
| `dim` | `int` |  Model default   | For OpenAI's newer text-embedding-3 model, we can specify a dimensionality that is smaller than the 1536 size. This feature supports it |


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
[Instructor](https://instructor-embedding.github.io/) is an instruction-finetuned text embedding model that can generate text embeddings tailored to any task (e.g. classification, retrieval, clustering, text evaluation, etc.) and domains (e.g. science, finance, etc.) by simply providing the task instruction, without any finetuning.

If you want to calculate customized embeddings for specific sentences, you can follow the unified template to write instructions.

!!! info
    Represent the `domain` `text_type` for `task_objective`:

    * `domain` is optional, and it specifies the domain of the text, e.g. science, finance, medicine, etc.
    * `text_type` is required, and it specifies the encoding unit, e.g. sentence, document, paragraph, etc.
    * `task_objective` is optional, and it specifies the objective of embedding, e.g. retrieve a document, classify the sentence, etc.

More information about the model can be found at the [source URL](https://github.com/xlang-ai/instructor-embedding).

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

### Gemini Embeddings
With Google's Gemini, you can represent text (words, sentences, and blocks of text) in a vectorized form, making it easier to compare and contrast embeddings. For example, two texts that share a similar subject matter or sentiment should have similar embeddings, which can be identified through mathematical comparison techniques such as cosine similarity. For more on how and why you should use embeddings, refer to the Embeddings guide.
The Gemini Embedding Model API supports various task types:

| Task Type               | Description                                                                                                                                                |
|-------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------|
| "`retrieval_query`"     | Specifies the given text is a query in a search/retrieval setting.                                                                                         |
| "`retrieval_document`"  | Specifies the given text is a document in a search/retrieval setting. Using this task type requires a title but is automatically proided by Embeddings API |
| "`semantic_similarity`" | Specifies the given text will be used for Semantic Textual Similarity (STS).                                                                               |
| "`classification`"      | Specifies that the embeddings will be used for classification.                                                                                             |
| "`clusering`"           | Specifies that the embeddings will be used for clustering.                                                                                                 |


Usage Example:

```python
import lancedb
import pandas as pd
from lancedb.pydantic import LanceModel, Vector
from lancedb.embeddings import get_registry


model = get_registry().get("gemini-text").create()

class TextModel(LanceModel):
    text: str = model.SourceField()
    vector: Vector(model.ndims()) = model.VectorField()

df = pd.DataFrame({"text": ["hello world", "goodbye world"]})
db = lancedb.connect("~/.lancedb")
tbl = db.create_table("test", schema=TextModel, mode="overwrite")

tbl.add(df)
rs = tbl.search("hello").limit(1).to_pandas()
```

### AWS Bedrock Text Embedding Functions
AWS Bedrock supports multiple base models for generating text embeddings. You need to setup the AWS credentials to use this embedding function.
You can do so by using `awscli` and also add your session_token:
```shell
aws configure
aws configure set aws_session_token "<your_session_token>"
```
to ensure that the credentials are set up correctly, you can run the following command:
```shell
aws sts get-caller-identity
```

Supported Embedding modelIDs are:
* `amazon.titan-embed-text-v1`
* `cohere.embed-english-v3`
* `cohere.embed-multilingual-v3`

Supported parameters (to be passed in `create` method) are:

| Parameter | Type | Default Value | Description |
|---|---|---|---|
| **name** | str | "amazon.titan-embed-text-v1" | The model ID of the bedrock model to use. Supported base models for Text Embeddings: amazon.titan-embed-text-v1, cohere.embed-english-v3, cohere.embed-multilingual-v3 |
| **region** | str | "us-east-1" | Optional name of the AWS Region in which the service should be called (e.g., "us-east-1"). |
| **profile_name** | str | None | Optional name of the AWS profile to use for calling the Bedrock service. If not specified, the default profile will be used. |
| **assumed_role** | str | None | Optional ARN of an AWS IAM role to assume for calling the Bedrock service. If not specified, the current active credentials will be used. |
| **role_session_name** | str | "lancedb-embeddings" | Optional name of the AWS IAM role session to use for calling the Bedrock service. If not specified, a "lancedb-embeddings" name will be used. |
| **runtime** | bool | True | Optional choice of getting different client to perform operations with the Amazon Bedrock service. |
| **max_retries** | int | 7 | Optional number of retries to perform when a request fails. |

Usage Example:

```python
model = get_registry().get("bedrock-text").create()

class TextModel(LanceModel):
    text: str = model.SourceField()
    vector: Vector(model.ndims()) = model.VectorField()

df = pd.DataFrame({"text": ["hello world", "goodbye world"]})
db = lancedb.connect("tmp_path")
tbl = db.create_table("test", schema=TextModel, mode="overwrite")

tbl.add(df)
rs = tbl.search("hello").limit(1).to_pandas()
```

## Multi-modal embedding functions
Multi-modal embedding functions allow you to query your table using both images and text.

### OpenClip embeddings
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

### Imagebind embeddings
We have support for [imagebind](https://github.com/facebookresearch/ImageBind) model embeddings. You can download our version of the packaged model via - `pip install imagebind-packaged==0.1.2`.

This function is registered as `imagebind` and supports Audio, Video and Text modalities(extending to Thermal,Depth,IMU data):

| Parameter | Type | Default Value | Description |
|---|---|---|---|
| `name` | `str` | `"imagebind_huge"` | Name of the model. |
| `device` | `str` | `"cpu"` | The device to run the model on. Can be `"cpu"` or `"gpu"`. |
| `normalize` | `bool` | `False` | set to `True` to normalize your inputs before model ingestion. |

Below is an example demonstrating how the API works:

```python
db = lancedb.connect(tmp_path)
registry = EmbeddingFunctionRegistry.get_instance()
func = registry.get("imagebind").create()

class ImageBindModel(LanceModel):
    text: str
    image_uri: str = func.SourceField()
    audio_path: str
    vector: Vector(func.ndims()) = func.VectorField()

# add locally accessible image paths
text_list=["A dog.", "A car", "A bird"]
image_paths=[".assets/dog_image.jpg", ".assets/car_image.jpg", ".assets/bird_image.jpg"]
audio_paths=[".assets/dog_audio.wav", ".assets/car_audio.wav", ".assets/bird_audio.wav"]

# Load data
inputs = [
    {"text": a, "audio_path": b, "image_uri": c}
    for a, b, c in zip(text_list, audio_paths, image_paths)
]

#create table and add data
table = db.create_table("img_bind", schema=ImageBindModel)
table.add(inputs)
```

Now, we can search using any modality:

#### image search
```python
query_image = "./assets/dog_image2.jpg" #download an image and enter that path here
actual = table.search(query_image).limit(1).to_pydantic(ImageBindModel)[0]
print(actual.text == "dog")
```
#### audio search

```python
query_audio = "./assets/car_audio2.wav" #download an audio clip and enter path here
actual = table.search(query_audio).limit(1).to_pydantic(ImageBindModel)[0]
print(actual.text == "car")
```
#### Text search
You can add any input query and fetch the result as follows:
```python
query = "an animal which flies and tweets" 
actual = table.search(query).limit(1).to_pydantic(ImageBindModel)[0]
print(actual.text == "bird")
```

If you have any questions about the embeddings API, supported models, or see a relevant model missing, please raise an issue [on GitHub](https://github.com/lancedb/lancedb/issues).
