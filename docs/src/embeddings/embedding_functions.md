Representing multi-modal data as vector embeddings is becoming a standard practice. Embedding functions themselves be thought of as a part of the processing pipeline that each request(input) has to be passed through. After initial setup these components are not expected to change for a particular project. 

This is main motivation behind our new embedding functions API, that allow you simply set it up once and the table remembers it, effectively making the **embedding functions disappear in the background** so you don't have to worry about modelling and simply focus on the DB aspects of VectorDB.


You can simply follow these steps and forget about the details of your embedding functions as long as you don't intend to change it.

### Step 1 - Define the embedding function
We have some pre-defined embedding functions in the global registry with more coming soon. Here's let's an implementation of CLIP as example.
```
registry = EmbeddingFunctionRegistry.get_instance()
clip = registry.get("open-clip").create()

```
You can also define your own embedding function by implementing the `EmbeddingFunction` abstract base interface. It subclasses PyDantic Model which can be utilized to write complex schemas simply as we'll see next!

### Step 2 - Define the Data Model or Schema
Our embedding function from the previous section abstracts away all the details about the models and dimensions required to define the schema. You can simply set a feild as **source** or **vector** column. Here's how

```python
class Pets(LanceModel):
    vector: Vector(clip.ndims) = clip.VectorField()
    image_uri: str = clip.SourceField()

```
`VectorField` tells LanceDB to use the clip embedding function to generate query embeddings for `vector` column & `SourceField` tells that when adding data, automatically use the embedding function to encode `image_uri`.


### Step 3 - Create LanceDB Table
Now that we have chosen/defined our embedding function and the schema, we can create the table

```python
db = lancedb.connect("~/lancedb")
table = db.create_table("pets", schema=Pets)

```
That's it! We have ingested all the information needed to embed source and query inputs. We can now forget about the model and dimension details and start to build or VectorDB

### Step 4 - Ingest lots of data and run vector search!
Now you can just add the data and it'll be vectorized automatically

```python
table.add([{"image_uri": u} for u in uris])
```

Our OpenCLIP query embedding function support querying via both text and images. 

```python
result = table.search("dog")
```

Let's query an image

```python
p = Path("path/to/images/samoyed_100.jpg")
query_image = Image.open(p)
table.search(query_image)

```
### Rate limit Handling
`EmbeddingFunction` class wraps the calls for source and query embedding generation inside a rate limit handler that retries the requests with exponential backoff after successive failures. By default the maximum retires is set to 7. You can tune it by setting it to a different number or disable it by setting it to 0.
Example
----

```python
clip = registry.get("open-clip").create() # Defaults to 7 max retries
clip = registry.get("open-clip").create(max_retries=10) # Increase max retries to 10
clip = registry.get("open-clip").create(max_retries=0) # Retries disabled
````

NOTE:
Embedding functions can also fail due to other errors that have nothing to do with rate limits. This is why the error is also logged.

### A little fun with PyDantic
LanceDB is integrated with PyDantic. Infact we've used the integration in the above example to define the schema. It is also being used behing the scene by the embdding function API to ingest useful information as table metadata.
You can also use it for adding utility operations in the schema. For example, in our multi-modal example, you can search images using text or another image. Let us define a utility function to plot the image.
```python
class Pets(LanceModel):
    vector: Vector(clip.ndims) = clip.VectorField()
    image_uri: str = clip.SourceField()

    @property
    def image(self):
        return Image.open(self.image_uri)
```
Now, you can covert your search results to pydantic model and use this property.

```python
rs = table.search(query_image).limit(3).to_pydantic(Pets)
rs[2].image
```

![](../assets/dog_clip_output.png)

Now that you've the basic idea about LanceDB embedding function, let us now dive deeper into the API that you can use to implement your own embedding functions!
