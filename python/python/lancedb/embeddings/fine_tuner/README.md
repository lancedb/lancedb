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
import pandas as pd

from lancedb.embeddings.fine_tuner.llm import Openai
from lancedb.embeddings.fine_tuner.dataset import QADataset, TextChunk
from lancedb.pydantic import LanceModel, Vector
from llama_index.core import SimpleDirectoryReader
from llama_index.core.node_parser import SentenceSplitter
from llama_index.core.schema import MetadataMode
from lancedb.embeddings import get_registry

# 1. Create a QADataset object
url = "uber10k.pdf"
reader = SimpleDirectoryReader(input_files=url)
docs = reader.load_data()

parser = SentenceSplitter()
nodes = parser.get_nodes_from_documents(docs)

if os.path.exists(name):
    ds = QADataset.load(name)
else:
    llm = Openai()
    
    # convert Llama-index TextNode to TextChunk
    chunks = [TextChunk.from_llama_index_node(node) for node in nodes]

    ds = QADataset.from_llm(chunks, llm)
    ds.save(name)

# 2. Initialize the embedding model
model = get_registry().get("sentence-transformers").create()

# 3. Fine-tune the model
model.finetune(trainset=ds, path="model_finetuned", epochs=4)

# 4. Evaluate the fine-tuned model
base = get_registry().get("sentence-transformers").create()
tuned = get_registry().get("sentence-transformers").create(name="./model_finetuned_1")
openai = get_registry().get("openai").create(name="text-embedding-3-large")


rs1 = base.evaluate(trainset, path="val_res")
rs2 = tuned.evaluate(trainset, path="val_res")
rs3 = openai.evaluate(trainset)

print("openai-embedding-v3 hit-rate  - ", pd.DataFrame(rs3)["is_hit"].mean())
print("fine-tuned hit-rate  - ", pd.DataFrame(rs2)["is_hit"].mean())
print("Base model hite-rate - ", pd.DataFrame(rs1)["is_hit"].mean())
```


