# Jina Embeddings

Jina embeddings are used to generate embeddings for text and image data.
You also need to set the `JINA_API_KEY` environment variable to use the Jina API.

You can find a list of supported models under [https://jina.ai/embeddings/](https://jina.ai/embeddings/)

Supported parameters (to be passed in `create` method) are:

| Parameter | Type | Default Value | Description |
|---|---|---|---|
| `name` | `str` | `"jina-clip-v1"` | The model ID of the jina model to use |

Usage Example:

```python
    import os
    import lancedb
    from lancedb.pydantic import LanceModel, Vector
    from lancedb.embeddings import EmbeddingFunctionRegistry

    os.environ['JINA_API_KEY'] = 'jina_*'

    jina_embed = EmbeddingFunctionRegistry.get_instance().get("jina").create(name="jina-embeddings-v2-base-en")


    class TextModel(LanceModel):
        text: str = jina_embed.SourceField()
        vector: Vector(jina_embed.ndims()) = jina_embed.VectorField()


    data = [{"text": "hello world"},
            {"text": "goodbye world"}]

    db = lancedb.connect("~/.lancedb-2")
    tbl = db.create_table("test", schema=TextModel, mode="overwrite")

    tbl.add(data)
```
