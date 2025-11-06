# Jina Embeddings : Multimodal

Jina embeddings can also be used to embed both text and image data, only some of the models support image data and you can check the list
under [https://jina.ai/embeddings/](https://jina.ai/embeddings/)

Supported parameters (to be passed in `create` method) are:

| Parameter | Type | Default Value | Description |
|---|---|---|---|
| `name` | `str` | `"jina-clip-v1"` | The model ID of the jina model to use |

Usage Example:

```python
    import os
    import requests
    import lancedb
    from lancedb.pydantic import LanceModel, Vector
    from lancedb.embeddings import get_registry
    import pandas as pd

    os.environ['JINA_API_KEY'] = 'jina_*'

    db = lancedb.connect("~/.lancedb")
    func = get_registry().get("jina").create()


    class Images(LanceModel):
        label: str
        image_uri: str = func.SourceField()  # image uri as the source
        image_bytes: bytes = func.SourceField()  # image bytes as the source
        vector: Vector(func.ndims()) = func.VectorField()  # vector column
        vec_from_bytes: Vector(func.ndims()) = func.VectorField()  # Another vector column


    table = db.create_table("images", schema=Images)
    labels = ["cat", "cat", "dog", "dog", "horse", "horse"]
    uris = [
        "http://farm1.staticflickr.com/53/167798175_7c7845bbbd_z.jpg",
        "http://farm1.staticflickr.com/134/332220238_da527d8140_z.jpg",
        "http://farm9.staticflickr.com/8387/8602747737_2e5c2a45d4_z.jpg",
        "http://farm5.staticflickr.com/4092/5017326486_1f46057f5f_z.jpg",
        "http://farm9.staticflickr.com/8216/8434969557_d37882c42d_z.jpg",
        "http://farm6.staticflickr.com/5142/5835678453_4f3a4edb45_z.jpg",
    ]
    # get each uri as bytes
    image_bytes = [requests.get(uri).content for uri in uris]
    table.add(
      pd.DataFrame({"label": labels, "image_uri": uris, "image_bytes": image_bytes})
    )
```
