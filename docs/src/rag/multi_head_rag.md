**Multi-Head RAG 📃**
====================================================================

Multi-head RAG (MRAG) is designed to handle queries that need multiple documents with diverse content. These queries are tough because the documents’ embeddings can be far apart, making retrieval difficult. MRAG simplifies this by using the activations from a Transformer's multi-head attention layer, rather than the decoder layer, to fetch these varied documents. Different attention heads capture different aspects of the data, so using these activations helps create embeddings that better represent various data facets and improves retrieval accuracy for complex queries.

**[Official Paper](https://arxiv.org/pdf/2406.05085)**

<figure markdown="span">
  ![agent-based-rag](https://raw.githubusercontent.com/lancedb/assets/main/docs/assets/rag/mrag-paper.png)
  <figcaption>Multi-Head RAG: <a href="https://github.com/spcl/MRAG">Source</a>
  </figcaption>
</figure>

MRAG is cost-effective and energy-efficient because it avoids extra LLM queries, multiple model instances, increased storage, and additional inference passes.

**[Official Implementation](https://github.com/spcl/MRAG)**

Here’s a code snippet for defining different embedding spaces with the [Embedding API](https://lancedb.github.io/lancedb/embeddings/embedding_functions/)

```python
import lancedb
from lancedb.pydantic import LanceModel, Vector
from lancedb.embeddings import get_registry

# model definition using LanceDB Embedding API
model1 = get_registry().get("openai").create()
model2 = get_registry().get("ollama").create(name="llama3")
model3 = get_registry().get("ollama").create(name="mistral")


# define schema for creating embedding spaces with Embedding API
class Space1(LanceModel):
    text: str = model1.SourceField()
    vector: Vector(model1.ndims()) = model1.VectorField()


class Space2(LanceModel):
    text: str = model2.SourceField()
    vector: Vector(model2.ndims()) = model2.VectorField()


class Space3(LanceModel):
    text: str = model3.SourceField()
    vector: Vector(model3.ndims()) = model3.VectorField()
```

Create different tables using defined embedding spaces, then make queries to each embedding space. Use the resulted closest documents from each embedding space to generate answers.


