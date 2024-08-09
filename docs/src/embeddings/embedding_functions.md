Representing multi-modal data as vector embeddings is becoming a standard practice. Embedding functions can themselves be thought of as key part of the data processing pipeline that each request has to be passed through. The assumption here is: after initial setup, these components and the underlying methodology are not expected to change for a particular project.

For this purpose, LanceDB introduces an **embedding functions API**, that allow you simply set up once, during the configuration stage of your project. After this, the table remembers it, effectively making the embedding functions *disappear in the background* so you don't have to worry about manually passing callables, and instead, simply focus on the rest of your data engineering pipeline.

!!! Note "Embedding functions on LanceDB cloud"
    When using embedding functions with LanceDB cloud, the embeddings will be generated on the source device and sent to the cloud. This means that the source device must have the necessary resources to generate the embeddings.

!!! warning
    Using the embedding function registry means that you don't have to explicitly generate the embeddings yourself.
    However, if your embedding function changes, you'll have to re-configure your table with the new embedding function
    and regenerate the embeddings. In the future, we plan to support the ability to change the embedding function via
    table metadata and have LanceDB automatically take care of regenerating the embeddings.


## 1. Define the embedding function

=== "Python"
    In the LanceDB python SDK, we define a global embedding function registry with
    many different embedding models and even more coming soon.
    Here's let's an implementation of CLIP as example.

    ```python
    from lancedb.embeddings import get_registry

    registry = get_registry()
    clip = registry.get("open-clip").create()
    ```

    You can also define your own embedding function by implementing the `EmbeddingFunction`
    abstract base interface. It subclasses Pydantic Model which can be utilized to write complex schemas simply as we'll see next!

=== "TypeScript"
    In the TypeScript SDK, the choices are more limited. For now, only the OpenAI
    embedding function is available.

    ```javascript
    import * as lancedb from '@lancedb/lancedb'
    import { getRegistry } from '@lancedb/lancedb/embeddings'

    // You need to provide an OpenAI API key
    const apiKey = "sk-..."
    // The embedding function will create embeddings for the 'text' column
    const func = getRegistry().get("openai").create({apiKey})
    ```
=== "Rust"
    In the Rust SDK, the choices are more limited. For now, only the OpenAI
    embedding function is available. But unlike the Python and TypeScript SDKs, you need manually register the OpenAI embedding function.

    ```toml
    // Make sure to include the `openai` feature
    [dependencies]
    lancedb = {version = "*", features = ["openai"]}
    ```

    ```rust
    --8<-- "rust/lancedb/examples/openai.rs:imports"
    --8<-- "rust/lancedb/examples/openai.rs:openai_embeddings"
    ```

## 2. Define the data model or schema

=== "Python"
    The embedding function defined above abstracts away all the details about the models and dimensions required to define the schema. You can simply set a field as **source** or **vector** column. Here's how:

    ```python
    class Pets(LanceModel):
        vector: Vector(clip.ndims()) = clip.VectorField()
        image_uri: str = clip.SourceField()
    ```

    `VectorField` tells LanceDB to use the clip embedding function to generate query embeddings for the `vector` column and `SourceField` ensures that when adding data, we automatically use the specified embedding function to encode `image_uri`.

=== "TypeScript"

    For the TypeScript SDK, a schema can be inferred from input data, or an explicit
    Arrow schema can be provided.

## 3. Create table and add data

Now that we have chosen/defined our embedding function and the schema,
we can create the table and ingest data without needing to explicitly generate
the embeddings at all:

=== "Python"
    ```python
    db = lancedb.connect("~/lancedb")
    table = db.create_table("pets", schema=Pets)

    table.add([{"image_uri": u} for u in uris])
    ```

=== "TypeScript"

    === "@lancedb/lancedb"

        ```ts
        --8<-- "nodejs/examples/embedding.ts:imports"
        --8<-- "nodejs/examples/embedding.ts:embedding_function"
        ```

    === "vectordb (deprecated)"

        ```ts
        const db = await lancedb.connect("data/sample-lancedb");
        const data = [
            { text: "pepperoni"},
            { text: "pineapple"}
        ]

        const table = await db.createTable("vectors", data, embedding)
        ```

## 4. Querying your table
Not only can you forget about the embeddings during ingestion, you also don't
need to worry about it when you query the table:

=== "Python"

    Our OpenCLIP query embedding function supports querying via both text and images:

    ```python
    results = (
        table.search("dog")
            .limit(10)
            .to_pandas()
    )
    ```

    Or we can search using an image:

    ```python
    p = Path("path/to/images/samoyed_100.jpg")
    query_image = Image.open(p)
    results = (
        table.search(query_image)
            .limit(10)
            .to_pandas()
    )
    ```

    Both of the above snippet returns a pandas DataFrame with the 10 closest vectors to the query.

=== "TypeScript"

    === "@lancedb/lancedb"

        ```ts
        const results = await table.search("What's the best pizza topping?")
            .limit(10)
            .toArray()
        ```

    === "vectordb (deprecated)

        ```ts
        const results = await table
            .search("What's the best pizza topping?")
            .limit(10)
            .execute()
        ```

    The above snippet returns an array of records with the top 10 nearest neighbors to the query.

---

## Rate limit Handling
`EmbeddingFunction` class wraps the calls for source and query embedding generation inside a rate limit handler that retries the requests with exponential backoff after successive failures. By default, the maximum retires is set to 7. You can tune it by setting it to a different number, or disable it by setting it to 0.

An example of how to do this is shown below:

```python
clip = registry.get("open-clip").create() # Defaults to 7 max retries
clip = registry.get("open-clip").create(max_retries=10) # Increase max retries to 10
clip = registry.get("open-clip").create(max_retries=0) # Retries disabled
```

!!! note
    Embedding functions can also fail due to other errors that have nothing to do with rate limits.
    This is why the error is also logged.

## Some fun with Pydantic

LanceDB is integrated with Pydantic, which was used in the example above to define the schema in Python. It's also used behind the scenes by the embedding function API to ingest useful information as table metadata.

You can also use the integration for adding utility operations in the schema. For example, in our multi-modal example, you can search images using text or another image. Let's define a utility function to plot the image.

```python
class Pets(LanceModel):
    vector: Vector(clip.ndims()) = clip.VectorField()
    image_uri: str = clip.SourceField()

    @property
    def image(self):
        return Image.open(self.image_uri)
```
Now, you can covert your search results to a Pydantic model and use this property.

```python
rs = table.search(query_image).limit(3).to_pydantic(Pets)
rs[2].image
```

![](../assets/dog_clip_output.png)

Now that you have the basic idea about LanceDB embedding functions and the embedding function registry,
let's dive deeper into defining your own [custom functions](./custom_embedding_function.md).
