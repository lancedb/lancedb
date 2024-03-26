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


??? "Check out available sentence-transformer models here!"
    ```markdown
    - sentence-transformers/all-MiniLM-L12-v2
    - sentence-transformers/paraphrase-mpnet-base-v2
    - sentence-transformers/gtr-t5-base
    - sentence-transformers/LaBSE
    - sentence-transformers/all-MiniLM-L6-v2
    - sentence-transformers/bert-base-nli-max-tokens
    - sentence-transformers/bert-base-nli-mean-tokens
    - sentence-transformers/bert-base-nli-stsb-mean-tokens
    - sentence-transformers/bert-base-wikipedia-sections-mean-tokens
    - sentence-transformers/bert-large-nli-cls-token
    - sentence-transformers/bert-large-nli-max-tokens
    - sentence-transformers/bert-large-nli-mean-tokens
    - sentence-transformers/bert-large-nli-stsb-mean-tokens
    - sentence-transformers/distilbert-base-nli-max-tokens
    - sentence-transformers/distilbert-base-nli-mean-tokens
    - sentence-transformers/distilbert-base-nli-stsb-mean-tokens
    - sentence-transformers/distilroberta-base-msmarco-v1
    - sentence-transformers/distilroberta-base-msmarco-v2
    - sentence-transformers/nli-bert-base-cls-pooling
    - sentence-transformers/nli-bert-base-max-pooling
    - sentence-transformers/nli-bert-base
    - sentence-transformers/nli-bert-large-cls-pooling
    - sentence-transformers/nli-bert-large-max-pooling
    - sentence-transformers/nli-bert-large
    - sentence-transformers/nli-distilbert-base-max-pooling
    - sentence-transformers/nli-distilbert-base
    - sentence-transformers/nli-roberta-base
    - sentence-transformers/nli-roberta-large
    - sentence-transformers/roberta-base-nli-mean-tokens
    - sentence-transformers/roberta-base-nli-stsb-mean-tokens
    - sentence-transformers/roberta-large-nli-mean-tokens
    - sentence-transformers/roberta-large-nli-stsb-mean-tokens
    - sentence-transformers/stsb-bert-base
    - sentence-transformers/stsb-bert-large
    - sentence-transformers/stsb-distilbert-base
    - sentence-transformers/stsb-roberta-base
    - sentence-transformers/stsb-roberta-large
    - sentence-transformers/xlm-r-100langs-bert-base-nli-mean-tokens
    - sentence-transformers/xlm-r-100langs-bert-base-nli-stsb-mean-tokens
    - sentence-transformers/xlm-r-base-en-ko-nli-ststb
    - sentence-transformers/xlm-r-bert-base-nli-mean-tokens
    - sentence-transformers/xlm-r-bert-base-nli-stsb-mean-tokens
    - sentence-transformers/xlm-r-large-en-ko-nli-ststb
    - sentence-transformers/bert-base-nli-cls-token
    - sentence-transformers/all-distilroberta-v1
    - sentence-transformers/multi-qa-MiniLM-L6-dot-v1
    - sentence-transformers/multi-qa-distilbert-cos-v1
    - sentence-transformers/multi-qa-distilbert-dot-v1
    - sentence-transformers/multi-qa-mpnet-base-cos-v1
    - sentence-transformers/multi-qa-mpnet-base-dot-v1
    - sentence-transformers/nli-distilroberta-base-v2
    - sentence-transformers/all-MiniLM-L6-v1
    - sentence-transformers/all-mpnet-base-v1
    - sentence-transformers/all-mpnet-base-v2
    - sentence-transformers/all-roberta-large-v1
    - sentence-transformers/allenai-specter
    - sentence-transformers/average_word_embeddings_glove.6B.300d
    - sentence-transformers/average_word_embeddings_glove.840B.300d
    - sentence-transformers/average_word_embeddings_komninos
    - sentence-transformers/average_word_embeddings_levy_dependency
    - sentence-transformers/clip-ViT-B-32-multilingual-v1
    - sentence-transformers/clip-ViT-B-32
    - sentence-transformers/distilbert-base-nli-stsb-quora-ranking
    - sentence-transformers/distilbert-multilingual-nli-stsb-quora-ranking
    - sentence-transformers/distilroberta-base-paraphrase-v1
    - sentence-transformers/distiluse-base-multilingual-cased-v1
    - sentence-transformers/distiluse-base-multilingual-cased-v2
    - sentence-transformers/distiluse-base-multilingual-cased
    - sentence-transformers/facebook-dpr-ctx_encoder-multiset-base
    - sentence-transformers/facebook-dpr-ctx_encoder-single-nq-base
    - sentence-transformers/facebook-dpr-question_encoder-multiset-base
    - sentence-transformers/facebook-dpr-question_encoder-single-nq-base
    - sentence-transformers/gtr-t5-large
    - sentence-transformers/gtr-t5-xl
    - sentence-transformers/gtr-t5-xxl
    - sentence-transformers/msmarco-MiniLM-L-12-v3
    - sentence-transformers/msmarco-MiniLM-L-6-v3
    - sentence-transformers/msmarco-MiniLM-L12-cos-v5
    - sentence-transformers/msmarco-MiniLM-L6-cos-v5
    - sentence-transformers/msmarco-bert-base-dot-v5
    - sentence-transformers/msmarco-bert-co-condensor
    - sentence-transformers/msmarco-distilbert-base-dot-prod-v3
    - sentence-transformers/msmarco-distilbert-base-tas-b
    - sentence-transformers/msmarco-distilbert-base-v2
    - sentence-transformers/msmarco-distilbert-base-v3
    - sentence-transformers/msmarco-distilbert-base-v4
    - sentence-transformers/msmarco-distilbert-cos-v5
    - sentence-transformers/msmarco-distilbert-dot-v5
    - sentence-transformers/msmarco-distilbert-multilingual-en-de-v2-tmp-lng-aligned
    - sentence-transformers/msmarco-distilbert-multilingual-en-de-v2-tmp-trained-scratch
    - sentence-transformers/msmarco-distilroberta-base-v2
    - sentence-transformers/msmarco-roberta-base-ance-firstp
    - sentence-transformers/msmarco-roberta-base-v2
    - sentence-transformers/msmarco-roberta-base-v3
    - sentence-transformers/multi-qa-MiniLM-L6-cos-v1
    - sentence-transformers/nli-mpnet-base-v2
    - sentence-transformers/nli-roberta-base-v2
    - sentence-transformers/nq-distilbert-base-v1
    - sentence-transformers/paraphrase-MiniLM-L12-v2
    - sentence-transformers/paraphrase-MiniLM-L3-v2
    - sentence-transformers/paraphrase-MiniLM-L6-v2
    - sentence-transformers/paraphrase-TinyBERT-L6-v2
    - sentence-transformers/paraphrase-albert-base-v2
    - sentence-transformers/paraphrase-albert-small-v2
    - sentence-transformers/paraphrase-distilroberta-base-v1
    - sentence-transformers/paraphrase-distilroberta-base-v2
    - sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2
    - sentence-transformers/paraphrase-multilingual-mpnet-base-v2
    - sentence-transformers/paraphrase-xlm-r-multilingual-v1
    - sentence-transformers/quora-distilbert-base
    - sentence-transformers/quora-distilbert-multilingual
    - sentence-transformers/sentence-t5-base
    - sentence-transformers/sentence-t5-large
    - sentence-transformers/sentence-t5-xxl
    - sentence-transformers/sentence-t5-xl
    - sentence-transformers/stsb-distilroberta-base-v2
    - sentence-transformers/stsb-mpnet-base-v2
    - sentence-transformers/stsb-roberta-base-v2
    - sentence-transformers/stsb-xlm-r-multilingual
    - sentence-transformers/xlm-r-distilroberta-base-paraphrase-v1
    - sentence-transformers/clip-ViT-L-14
    - sentence-transformers/clip-ViT-B-16
    - sentence-transformers/use-cmlm-multilingual
    - sentence-transformers/all-MiniLM-L12-v1
    ```

!!! info
    You can also load many other model architectures from the library. For example models from sources such as BAAI, nomic, salesforce research, etc.
    See this HF hub page for all [supported models](https://huggingface.co/models?library=sentence-transformers).

!!! note "BAAI Embeddings example"
    Here is an example that uses BAAI embedding model from the HuggingFace Hub [supported models](https://huggingface.co/models?library=sentence-transformers)
    ```python
    db = lancedb.connect("/tmp/db")
    registry = EmbeddingFunctionRegistry.get_instance()
    model = registry.get("sentence-transformers").create(name="BAAI/bge-small-en-v1.5", device="cpu")

    class Words(LanceModel):
        text: str = model.SourceField()
        vector: Vector(model.ndims()) = model.VectorField()

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
Visit sentence-transformers [HuggingFace HUB](https://huggingface.co/sentence-transformers) page for more information on the available models.


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
