### Fine-tuning workflow
The fine-tuning workflow is as follows:
1. Create a `QADataset` object.
2. Initialize any embedding function using LanceDB embedding API
3. Call `finetune` method on the embedding object with the `QADataset` object as an argument.
4. Evaluate the fine-tuned model using the `evaluate` method in the embedding API.

# End-to-End Examples
The following is an example of how to fine-tune an embedding model using the LanceDB embedding API.

## Example 1: Fine-tuning from a synthetic dataset

```python
import os
import pandas as pd

from lancedb.embeddings.fine_tuner.llm import Openai
from lancedb.embeddings.fine_tuner.dataset import QADataset, TextChunk
from lancedb.pydantic import LanceModel, Vector
from llama_index.core import SimpleDirectoryReader
from llama_index.core.node_parser import SentenceSplitter
from llama_index.core.schema import MetadataMode
from lancedb.embeddings import get_registry

dataset = "Uber10KDataset2021"
lance_dataset_dir = dataset + "_lance"
valset_dir = dataset + "_lance_val"
finetuned_model_path = "./model_finetuned"

# 1. Create a QADataset object. See all datasets on llama-index here: https://github.com/run-llama/llama_index/tree/main/llama-datasets

if not os.path.exists(f"./data/{dataset}"):
    os.system(
        f"llamaindex-cli download-llamadataset {dataset} --download-dir ./data/{dataset}"
    )
docs = SimpleDirectoryReader(input_dir=f"./data/{dataset}/source_files").load_data()

parser = SentenceSplitter()
nodes = parser.get_nodes_from_documents(docs)
# convert Llama-index TextNode to TextChunk
chunks = [TextChunk.from_llama_index_node(node) for node in nodes]
llm = Openai()

if os.path.exists(lance_dataset_dir):
    trainset = QADataset.load(lance_dataset_dir)
else:
    trainset = QADataset.from_llm(chunks, llm, num_questions_per_chunk=2)
    trainset.save(lance_dataset_dir)

# Ideally, we should have a standard dataset for validation, but here we're just generating a synthetic dataset.
if os.path.exists(valset_dir):
    valset = QADataset.load(valset_dir)
else:
    valset = QADataset.from_llm(chunks, llm, num_questions_per_chunk=4)
    valset.save(valset_dir)

# 2. Initialize the embedding model
model = get_registry().get("sentence-transformers").create(name="sentence-transformers/multi-qa-MiniLM-L6-cos-v1")

# 3. Fine-tune the model
model.finetune(trainset=trainset, path=finetuned_model_path, epochs=4)

# 4. Evaluate the fine-tuned model
base = get_registry().get("sentence-transformers").create(name="sentence-transformers/multi-qa-MiniLM-L6-cos-v1")
base_results = base.evaluate(valset, top_k=5)

tuned = get_registry().get("sentence-transformers").create(name=finetuned_model_path)
tuned_results = tuned.evaluate(valset, top_k=5)

openai = get_registry().get("openai").create(name="text-embedding-3-small")
openai_results = openai.evaluate(valset, top_k=5)


print("openai-embedding-v3 hit-rate  - ", pd.DataFrame(openai_results)["is_hit"].mean())
print("fine-tuned hit-rate  - ", pd.DataFrame(tuned_results)["is_hit"].mean())
print("Base model hite-rate - ", pd.DataFrame(base_results)["is_hit"].mean())
```

Fine-tuning workflow for embeddings consists for the following parts:

### QADataset
This class is used for managing the data for fine-tuning. It contains the following builder methods:
```
- from_llm(
        nodes: 'List[TextChunk]' ,
        llm: BaseLLM,
        qa_generate_prompt_tmpl: str = DEFAULT_PROMPT_TMPL,
        num_questions_per_chunk: int = 2,
) -> "QADataset"
```
Create synthetic data from a language model and text chunks of the original document on which the model is to be fine-tuned.

```python

from_responses(docs: List['TextChunk'], queries: Dict[str, str], relevant_docs: Dict[str, List[str]])-> "QADataset"
```
Create dataset from queries and responses based on a real-world scenario. Designed to be used for knowledge distillation from a larger LLM to a smaller one.

It also contains the following data attributes:
```
    queries (Dict[str, str]): Dict id -> query.
    corpus (Dict[str, str]): Dict id -> string.
    relevant_docs (Dict[str, List[str]]): Dict query id -> list of doc ids.
```

### TextChunk
This class is used for managing the data for fine-tuning. It is designed to allow working with and standardize various text splitting/pre-processing tools like llama-index and langchain. It contains the following attributes:
```
    text: str
    id: str
    metadata: Dict[str, Any] = {}
```

Builder Methods:

```python
from_llama_index_node(node) -> "TextChunk"
```
Create a text chunk from a llama index node.

```python
from_langchain_node(node) -> "TextChunk"
```
Create a text chunk from a langchain index node.

```python
from_chunk(cls, chunk: str, metadata: dict = {}) -> "TextChunk"
```
Create a text chunk from a string.

### FineTuner
This class is used for fine-tuning embeddings. It is exposed to the user via a high-level function in the base embedding api.
```python
class BaseEmbeddingTuner(ABC):
    """Base Embedding finetuning engine."""

    @abstractmethod
    def finetune(self) -> None:
        """Goes off and does stuff."""

    def helper(self) -> None:
        """A helper method."""
        pass
```

### Embedding API finetuning implementation
Each embedding API needs to implement `finetune` method in order to support fine-tuning. A vanilla evaluation technique has been implemented in the `BaseEmbedding` class that calculates hit_rate @ `top_k`.