## Implicit vectorization / Ingesting embedding functions
Representing multi-modal data as vector embeddings is becoming a standard practice. So much so that the Embedding functions themselves can be thought of as a part of the processing pipeline or a middleware that each request(input) has to be passed through. After initial setup these components are not expected to change for a particular project. This is main motivation behind our new embedding functions API, that allow you simply set it up once and the table remembers it, effectively making the **embedding functions disappear in the background** so you don't have to worry about modelling and simply focus on the DB aspects of VectorDB.
Abstracting away the details of embedding function becomes even more helpful in multi-modal cases where the same model has different pathways for data of different modality.

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

```
class Pets(LanceModel):
    vector: Vector(clip.ndims) = clip.VectorField()
    image_uri: str = clip.SourceField()
```
`VectorField` tells LanceDB to use the clip embedding function to generate query embeddings for `vector` column & `SourceField` tells that when adding data, automatically use the embedding function to encode `image_uri`.


### Step 3 - Create LanceDB Table
Now that we have chosen/defined our embedding function and the schema, we can now create the table

```
db = lancedb.connect("~/lancedb")
table = db.create_table("pets", schema=Pets)

```
That's it! We have now ingested all the information needed to embed source and query inputs. We can now forget about the model and dimension details and start to build or VectorDB

### Step 4 - Add lots of data and run vector search!
Now you can just add the data and it'll be vectorized automatically
```
table.add(pd.DataFrame({"image_uri": uris}))
```
