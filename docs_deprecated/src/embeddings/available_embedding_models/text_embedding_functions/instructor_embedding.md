# Instructor Embeddings
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