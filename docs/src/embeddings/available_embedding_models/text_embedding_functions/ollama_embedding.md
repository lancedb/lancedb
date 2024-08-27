# Ollama embeddings

Generate embeddings via the [ollama](https://github.com/ollama/ollama-python) python library. More details:

- [Ollama docs on embeddings](https://github.com/ollama/ollama/blob/main/docs/api.md#generate-embeddings)
- [Ollama blog on embeddings](https://ollama.com/blog/embedding-models)

| Parameter              | Type                       | Default Value            | Description                                                                                                                                    |
|------------------------|----------------------------|--------------------------|------------------------------------------------------------------------------------------------------------------------------------------------|
| `name`                 | `str`                      | `nomic-embed-text`       | The name of the model.                                                                                                                         |
| `host`                 | `str`                      | `http://localhost:11434` | The Ollama host to connect to.                                                                                                                 |
| `options`              | `ollama.Options` or `dict` | `None`                   | Additional model parameters listed in the documentation for the Modelfile such as `temperature`. |
| `keep_alive`           | `float` or `str`           | `"5m"`                   | Controls how long the model will stay loaded into memory following the request.                                                                |
| `ollama_client_kwargs` | `dict`                     | `{}`                     | kwargs that can be past to the `ollama.Client`.                                                                                                |

```python
import lancedb
from lancedb.pydantic import LanceModel, Vector
from lancedb.embeddings import get_registry

db = lancedb.connect("/tmp/db")
func = get_registry().get("ollama").create(name="nomic-embed-text")

class Words(LanceModel):
    text: str = func.SourceField()
    vector: Vector(func.ndims()) = func.VectorField()

table = db.create_table("words", schema=Words, mode="overwrite")
table.add([
    {"text": "hello world"},
    {"text": "goodbye world"}
])

query = "greetings"
actual = table.search(query).limit(1).to_pydantic(Words)[0]
print(actual.text)
```
