# Gemini Embeddings
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