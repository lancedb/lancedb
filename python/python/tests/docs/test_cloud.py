# --8<-- [start:imports]
import lancedb
import pyarrow as pa
import numpy as np

# --8<-- [end:imports]
# --8<-- [start:gen_data]
def gen_data(total_rows: int, ndims: int = 1536):
  return pa.RecordBatch.from_pylist(
    [
        {
            "vector": np.random.rand(ndims).astype(np.float32).tolist(),
            "id": i,
            "name": "name_"+str(i),
        }
        for i in range(total_rows)
    ],
).to_pandas()
# --8<-- [end:gen_data]

def test_cloud_quickstart():
    # --8<-- [start:connect]
    db = lancedb.connect(
        uri="db://your-project-slug",
        api_key="your-api-key",
        region="your-cloud-region"
    )
    # --8<-- [end:connect]
    # --8<-- [start:create_table]
    table_name = "myTable" 
    table = db.create_table(table_name, data=gen_data(5000))
    # --8<-- [end:create_table]
    # --8<-- [start:create_index_search]
    # create a vector index
    table.create_index("cosine", vector_column_name="vector")
    result = (
        table.search([0.01, 0.02])
        .select(["vector", "item"])
        .limit(1)
        .to_pandas()
    )
    # --8<-- [end:create_index_search]
    # --8<-- [start:drop_table]
    db.drop_table(table_name)
    # --8<-- [end:drop_table]

def test_ingest_data():
    # --8<-- [start:ingest_data]
    import lancedb
    import pyarrow as pa

    # connect to LanceDB
    db = lancedb.connect(
        uri="db://your-project-slug",
        api_key="your-api-key",
        region="us-east-1"
    )

    # create an empty table with schema
    table_name = "myTable"
    data = [
        {"vector": [3.1, 4.1], "item": "foo", "price": 10.0},
        {"vector": [5.9, 26.5], "item": "bar", "price": 20.0},
        {"vector": [10.2, 100.8], "item": "baz", "price": 30.0},
        {"vector": [1.4, 9.5], "item": "fred", "price": 40.0},
    ]
    schema = pa.schema([
        pa.field("vector", pa.list_(pa.float32(), 2)),
        pa.field("item", pa.utf8()),
        pa.field("price", pa.float32()),
    ])
    table = db.create_table(table_name, schema=schema)
    table.add(data)
    # --8<-- [end:ingest_data]
    # --8<-- [start:ingest_data_in_batch]
    def make_batches():
        for i in range(5):
            yield pa.RecordBatch.from_arrays(
                [
                    pa.array([[3.1, 4.1], [5.9, 26.5]],
                            pa.list_(pa.float32(), 2)),
                    pa.array(["foo", "bar"]),
                    pa.array([10.0, 20.0]),
                ],
                ["vector", "item", "price"],
            )

    schema = pa.schema([
        pa.field("vector", pa.list_(pa.float32(), 2)),
        pa.field("item", pa.utf8()),
        pa.field("price", pa.float32()),
    ])
    db.create_table("table2", make_batches(), schema=schema)
    # --8<-- [end:ingest_data_in_batch]

def test_updates():
    # --8<-- [start:update_data]
    import lancedb

    # connect to LanceDB
    db = lancedb.connect(
    uri="db://your-project-slug",
    api_key="your-api-key",
    region="us-east-1"
    )
    table_name = "myTable"
    table = db.open_table(table_name)
    table.update(where="price < 20.0", values={"vector": [2, 2], "item": "foo-updated"})
    # --8<-- [end:update_data]  
    # --8<-- [start:merge_insert]
    table = db.open_table(table_name)
    # upsert
    new_data = [
        {"vector": [1, 1], "item": 'foo-updated', "price": 50.0}
    ]
    table.merge_insert("item") \
        .when_matched_update_all() \
        .when_not_matched_insert_all() \
        .execute(new_data)
    # --8<-- [end:merge_insert]
    # --8<-- [start:delete_data]
    table_name = "myTable"
    table = db.open_table(table_name)
    # delete data
    predicate = "price = 30.0"
    table.delete(predicate)
    # --8<-- [end:delete_data]

def test_create_index():
    # --8<-- [start:create_index]
    import lancedb

    # connect to LanceDB
    db = lancedb.connect(
        uri="db://your-project-slug",
        api_key="your-api-key",
        region="us-east-1"
    )

    table_name = "myTable"
    table = db.open_table(table_name)
    # the vector column only needs to be specified when there are 
    # multiple vector columns or the column is not named as "vector"
    # L2 is used as the default distance metric
    table.create_index(metric="cosine", vector_column_name="vector")
    # --8<-- [end:create_index]

def test_create_scalar_index():
    # --8<-- [start:create_scalar_index]
    import lancedb

    # connect to LanceDB
    db = lancedb.connect(
    uri="db://your-project-slug",
    api_key="your-api-key",
    region="us-east-1"
    )

    table_name = "myTable"
    table = db.open_table(table_name)
    # default is BTree
    table.create_scalar_index("item", index_type="BITMAP")
    # --8<-- [end:create_scalar_index]

def test_create_fts_index():
    # --8<-- [start:create_fts_index]
    import lancedb

    # connect to LanceDB
    db = lancedb.connect(
    uri="db://your-project-slug",
    api_key="your-api-key",
    region="us-east-1"
    )

    table_name = "myTable"
    data = [
        {"vector": [3.1, 4.1], "text": "Frodo was a happy puppy"},
        {"vector": [5.9, 26.5], "text": "There are several kittens playing"},
    ]
    table = db.create_table(table_name, data=data)
    table.create_fts_index("text")
    # --8<-- [end:create_fts_index]

def test_search():
    # --8<-- [start:vector_search]
    import lancedb

    # connect to LanceDB
    db = lancedb.connect(
        uri="db://your-project-slug",
        api_key="your-api-key",
        region="us-east-1"
    )

    table_name = "myTable"
    table = db.open_table(table_name)
    query = [0.4, 1.4]
    result = (
        table.search(query)
        .where("price > 10.0", prefilter=True)
        .select(["item", "vector"])
        .limit(2)
        .to_pandas()
    )
    # --8<-- [end:vector_search]
    # --8<-- [start:full_text_search]
    import lancedb
    
    # connect to LanceDB
    db = lancedb.connect(
    uri="db://your-project-slug",
    api_key="your-api-key",
    region="us-east-1"
    )
    table_name = "myTable"
    table = db.create_table(
        table_name,
        data=[
            {"vector": [3.1, 4.1], "text": "Frodo was a happy puppy"},
            {"vector": [5.9, 26.5], "text": "There are several kittens playing"},
        ],
    )

    table.create_fts_index("text")
    table.search("puppy", query_type="fts").limit(10).select(["text"]).to_list()
    # --8<-- [end:full_text_search]
    # --8<-- [start:hybrid_search]
    import os

    import lancedb
    import openai
    from lancedb.embeddings import get_registry
    from lancedb.pydantic import LanceModel, Vector
    from lancedb.rerankers import RRFReranker

    # connect to LanceDB
    db = lancedb.connect(
    uri="db://your-project-slug",
    api_key="your-api-key",
    region="us-east-1"
    )

    # Configuring the environment variable OPENAI_API_KEY
    if "OPENAI_API_KEY" not in os.environ:
        # OR set the key here as a variable
        openai.api_key = "sk-..."
    embeddings = get_registry().get("openai").create()

    class Documents(LanceModel):
        text: str = embeddings.SourceField()
        vector: Vector(embeddings.ndims()) = embeddings.VectorField()

    table_name = "myTable"
    table = db.create_table(table_name, schema=Documents)
    data = [
        {"text": "rebel spaceships striking from a hidden base"},
        {"text": "have won their first victory against the evil Galactic Empire"},
        {"text": "during the battle rebel spies managed to steal secret plans"},
        {"text": "to the Empire's ultimate weapon the Death Star"},
    ]
    table.add(data=data)
    table.create_index("L2", "vector")
    table.create_fts_index("text")

    # you can use table.list_indices() to make sure indices have been created
    reranker = RRFReranker()
    results = (
        table.search(
            "flower moon",
            query_type="hybrid",
            vector_column_name="vector",
            fts_columns="text",
        )
        .rerank(reranker)
        .limit(10)
        .to_pandas()
    )
    # --8<-- [end:hybrid_search]

def test_filtering():
    # --8<-- [start:filtering]
    import lancedb

    # connect to LanceDB
    db = lancedb.connect(
    uri="db://your-project-slug",
    api_key="your-api-key",
    region="us-east-1"
    )
    table_name = "myTable"
    table = db.open_table(table_name)
    result = (
        table.search([100, 102])
        .where("(item IN ('foo', 'bar')) AND (price > 10.0)")
        .to_arrow()
    )
    # --8<-- [end:filtering]
    # --8<-- [start:sql_filtering]
    table.search([100, 102]) \
        .where("(item IN ('foo', 'bar')) AND (price > 10.0)") \
        .to_arrow()
    # --8<-- [end:sql_filtering]
    