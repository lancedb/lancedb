**Vanilla RAG 🌱**
====================================================================

RAG(Retrieval-Augmented Generation) works by finding documents related to the user's question, combining them with a prompt for a large language model (LLM), and then using the LLM to create more accurate and relevant answers.

Here’s a simple guide to building a RAG pipeline from scratch:

1. **Data Loading**: Gather and load the documents you want to use for answering questions.

2. **Chunking and Embedding**: Split the documents into smaller chunks and convert them into numerical vectors (embeddings) that capture their meaning.

3. **Vector Store**: Create a LanceDB table to store and manage these vectors for quick access during retrieval.

4. **Retrieval & Prompt Preparation**: When a question is asked, find the most relevant document chunks from the table and prepare a prompt combining these chunks with the question.

5. **Answer Generation**: Send the prepared prompt to a LLM to generate a detailed and accurate answer.

<figure markdown="span">
  ![agent-based-rag](https://raw.githubusercontent.com/lancedb/assets/main/docs/assets/rag/rag_from_scratch.png)
  <figcaption>Vanilla RAG
  </figcaption>
</figure>

[![Open In Colab](../assets/colab.svg)](https://colab.research.google.com/github/lancedb/vectordb-recipes/blob/main/tutorials/RAG-from-Scratch/RAG_from_Scratch.ipynb)

Here’s a code snippet for defining a table with the [Embedding API](https://lancedb.github.io/lancedb/embeddings/embedding_functions/), which simplifies the process by handling embedding extraction and querying in one step.

```python
import pandas as pd
import lancedb
from lancedb.pydantic import LanceModel, Vector
from lancedb.embeddings import get_registry

db = lancedb.connect("/tmp/db")
model = get_registry().get("sentence-transformers").create(name="BAAI/bge-small-en-v1.5", device="cpu")

class Docs(LanceModel):
    text: str = model.SourceField()
    vector: Vector(model.ndims()) = model.VectorField()

table = db.create_table("docs", schema=Docs)

# considering chunks are in list format
df = pd.DataFrame({'text':chunks})
table.add(data=df)

query = "What is issue date of lease?"
actual = table.search(query).limit(1).to_list()[0]
print(actual.text)
```

Check Colab for the complete code

[![Open In Colab](../assets/colab.svg)](https://colab.research.google.com/github/lancedb/vectordb-recipes/blob/main/tutorials/RAG-from-Scratch/RAG_from_Scratch.ipynb)