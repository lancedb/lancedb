import lancedb 
from lancedb.embeddings import get_registry
from lancedb.pydantic import LanceModel, Vector
import pandas as pd
import numpy as np

tmp_path = "/tmp/lancedb"
alias = "sentence-transformers"


model = get_registry().get("instructor").create()

class TextModel(LanceModel):
    text: str = model.SourceField()
    vector: Vector(model.ndims()) = model.VectorField()

df = pd.DataFrame({"text": ["hello world", "goodbye world"]})
db = lancedb.connect(tmp_path)
tbl = db.create_table("test", schema=TextModel, mode="overwrite")

tbl.add(df)
assert len(tbl.to_pandas()["vector"][0]) == model.ndims()

db = lancedb.connect(tmp_path)
registry = get_registry()
func = registry.get(alias).create()
func2 = registry.get(alias).create()

class Words(LanceModel):
    text: str = func.SourceField()
    text2: str = func2.SourceField()
    vector: Vector(func.ndims()) = func.VectorField()
    vector2: Vector(func2.ndims()) = func2.VectorField()

table = db.create_table("words", schema=Words, mode="overwrite")
table.add(
    pd.DataFrame(
        {
            "text": [
                "hello world",
                "goodbye world",
                "fizz",
                "buzz",
                "foo",
                "bar",
                "baz",
            ],
            "text2": [
                "to be or not to be",
                "that is the question",
                "for whether tis nobler",
                "in the mind to suffer",
                "the slings and arrows",
                "of outrageous fortune",
                "or to take arms",
            ],
        }
    )
)
table.add(
    pd.DataFrame(
        {
            "text": [
                "hello world",
                "goodbye world",
                "fizz",
                "buzz",
                "foo",
                "bar",
                "baz",
            ],
            "text2": [
                "to be or not to be",
                "that is the question",
                "for whether tis nobler",
                "in the mind to suffer",
                "the slings and arrows",
                "of outrageous fortune",
                "or to take arms",
            ],
        }
    )
)


query = "greetings"
actual = table.search(query).limit(1).to_pydantic(Words)[0]

vec = func.compute_query_embeddings(query)[0]
expected = table.search(vec).limit(1).to_pydantic(Words)[0]
assert actual.text == expected.text
assert actual.text == "hello world"
assert not np.allclose(actual.vector, actual.vector2)

actual = (
    table.search(query, vector_column_name="vector2").limit(1).to_pydantic(Words)[0]
)
assert actual.text != "hello world"
assert not np.allclose(actual.vector, actual.vector2)

import os
import sys
if os.environ.get("COHERE_API_KEY") is None:
    print("Skipping cohere tests")
    sys.exit(0)
cohere = get_registry().get("cohere").create(name="embed-multilingual-v2.0", max_retries=0)

class TextModel(LanceModel):
    text: str = cohere.SourceField()
    vector: Vector(cohere.ndims()) = cohere.VectorField()

df = pd.DataFrame({"text": ["hello world", "goodbye world"]})
db = lancedb.connect("~/lancedb")
tbl = db.create_table("test", schema=TextModel, mode="overwrite")

tbl.add(df)
assert len(tbl.to_pandas()["vector"][0]) == cohere.ndims()