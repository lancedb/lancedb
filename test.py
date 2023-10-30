import lancedb
from lancedb.pydantic import LanceModel, Vector
from lancedb.embeddings import get_registry, InstuctorEmbeddingFunction

instructor = get_registry().get("instructor").create()

class Schema(LanceModel):
    vector: Vector(instructor.ndims()) = instructor.VectorField()
    text: str = instructor.SourceField()

db = lancedb.connect("~/.lancedb")
tbl = db.create_table("test", schema=Schema, mode="overwrite")

texts = [{"text": "Capitalism has been dominant in the Western world since the end of feudalism, but most feel[who?] that the term 'mixed economies' more precisely describes most contemporary economies, due to their containing both private-owned and state-owned enterprises. In capitalism, prices determine the demand-supply scale. For example, higher demand for certain goods and services lead to higher prices and lower demand for certain goods lead to lower prices."},
         {"text": "The disparate impact theory is especially controversial under the Fair Housing Act because the Act regulates many activities relating to housing, insurance, and mortgage loansâ€”and some scholars have argued that the theory's use under the Fair Housing Act, combined with extensions of the Community Reinvestment Act, contributed to rise of sub-prime lending and the crash of the U.S. housing market and ensuing global economic recession"},
         {"text": "Represent the Wikipedia document for retrieval: ','Disparate impact in United States labor law refers to practices in employment, housing, and other areas that adversely affect one group of people of a protected characteristic more than another, even though rules applied by employers or landlords are formally neutral. Although the protected classes vary by statute, most federal civil rights laws protect based on race, color, religion, national origin, and sex as protected traits, and some laws include disability status and other traits as well."}]

tbl.add(texts)

rs = tbl.search("where is the food stored in a yam plant").to_df()
print(rs)
import pdb;pdb.set_trace()