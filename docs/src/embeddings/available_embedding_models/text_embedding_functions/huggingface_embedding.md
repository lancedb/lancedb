# Huggingface embedding models
We offer support for all Hugging Face models (which can be loaded via [transformers](https://huggingface.co/docs/transformers/en/index) library). The default model is `colbert-ir/colbertv2.0` which also has its own special callout - `registry.get("colbert")`. Some Hugging Face models might require custom models defined on the HuggingFace Hub in their own modeling files. You may enable this by setting `trust_remote_code=True`. This option should only be set to True for repositories you trust and in which you have read the code, as it will execute code present on the Hub on your local machine. 

Example usage - 
```python
import lancedb
import pandas as pd

from lancedb.embeddings import get_registry
from lancedb.pydantic import LanceModel, Vector

model = get_registry().get("huggingface").create(name='facebook/bart-base')

class Words(LanceModel):
    text: str = model.SourceField()
    vector: Vector(model.ndims()) = model.VectorField()

df = pd.DataFrame({"text": ["hi hello sayonara", "goodbye world"]})
table = db.create_table("greets", schema=Words)
table.add(df)
query = "old greeting"
actual = table.search(query).limit(1).to_pydantic(Words)[0]
print(actual.text)
```