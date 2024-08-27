# Cohere Embeddings

Using cohere API requires cohere package, which can be installed using `pip install cohere`. Cohere embeddings are used to generate embeddings for text data. The embeddings can be used for various tasks like semantic search, clustering, and classification.
You also need to set the `COHERE_API_KEY` environment variable to use the Cohere API.

Supported models are:
* embed-english-v3.0
* embed-multilingual-v3.0
* embed-english-light-v3.0
* embed-multilingual-light-v3.0
* embed-english-v2.0
* embed-english-light-v2.0
* embed-multilingual-v2.0


Supported parameters (to be passed in `create` method) are:

| Parameter | Type | Default Value | Description |
|---|---|--------|---------|
| `name` | `str` | `"embed-english-v2.0"` | The model ID of the cohere model to use. Supported base models for Text Embeddings: embed-english-v3.0, embed-multilingual-v3.0, embed-english-light-v3.0, embed-multilingual-light-v3.0, embed-english-v2.0, embed-english-light-v2.0, embed-multilingual-v2.0 |
| `source_input_type` | `str` | `"search_document"` | The type of input data to be used for the source column. |
| `query_input_type` | `str` | `"search_query"` | The type of input data to be used for the query. |

Cohere supports following input types:

| Input Type               | Description                          |
|-------------------------|---------------------------------------|
| "`search_document`"     | Used for embeddings stored in a vector|
|                         | database for search use-cases.        |
| "`search_query`"        | Used for embeddings of search queries |
|                         | run against a vector DB               |
| "`semantic_similarity`" | Specifies the given text will be used |
|                         | for Semantic Textual Similarity (STS) |
| "`classification`"      | Used for embeddings passed through a  |
|                         | text classifier.                      |
| "`clustering`"          | Used for the embeddings run through a |
|                         | clustering algorithm                  |

Usage Example:
    
```python
    import lancedb
    from lancedb.pydantic import LanceModel, Vector
    from lancedb.embeddings import EmbeddingFunctionRegistry

    cohere = EmbeddingFunctionRegistry
        .get_instance()
        .get("cohere")
        .create(name="embed-multilingual-v2.0")

    class TextModel(LanceModel):
        text: str = cohere.SourceField()
        vector: Vector(cohere.ndims()) =  cohere.VectorField()

    data = [ { "text": "hello world" },
            { "text": "goodbye world" }]

    db = lancedb.connect("~/.lancedb")
    tbl = db.create_table("test", schema=TextModel, mode="overwrite")

    tbl.add(data)
```