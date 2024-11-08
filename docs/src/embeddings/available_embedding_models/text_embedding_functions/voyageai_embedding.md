# VoyageAI Embeddings

Voyage AI provides cutting-edge embedding and rerankers.


Using voyageai API requires voyageai package, which can be installed using `pip install voyageai`. Voyage AI embeddings are used to generate embeddings for text data. The embeddings can be used for various tasks like semantic search, clustering, and classification.
You also need to set the `VOYAGE_API_KEY` environment variable to use the VoyageAI API.

Supported models are:

- voyage-3
- voyage-3-lite
- voyage-finance-2
- voyage-multilingual-2
- voyage-law-2
- voyage-code-2


Supported parameters (to be passed in `create` method) are:

| Parameter | Type | Default Value | Description |
|---|---|--------|---------|
| `name` | `str` | `"voyage-3"` | The model ID of the model to use. Supported base models for Text Embeddings: voyage-3, voyage-3-lite, voyage-finance-2, voyage-multilingual-2, voyage-law-2, voyage-code-2 |
| `input_type` | `str` | `None` | Type of the input text. Default to None. Other options: query, document. |
| `truncation` | `bool` | `True` | Whether to truncate the input texts to fit within the context length. |


Usage Example:
    
```python
    import lancedb
    from lancedb.pydantic import LanceModel, Vector
    from lancedb.embeddings import EmbeddingFunctionRegistry

    voyageai = EmbeddingFunctionRegistry
        .get_instance()
        .get("voyageai")
        .create(name="voyage-3")

    class TextModel(LanceModel):
        text: str = voyageai.SourceField()
        vector: Vector(voyageai.ndims()) =  voyageai.VectorField()

    data = [ { "text": "hello world" },
            { "text": "goodbye world" }]

    db = lancedb.connect("~/.lancedb")
    tbl = db.create_table("test", schema=TextModel, mode="overwrite")

    tbl.add(data)
```