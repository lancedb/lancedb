from abc import ABC, abstractmethod
from typing import Optional
from uuid import uuid4

import numpy as np
import pyarrow as pa
import pydantic

import lancedb
from lancedb.embeddings import with_embeddings
from .schema import vector, pydantic_to_schema, PYDANTIC_VERSION


class SearchableModel(ABC, pydantic.BaseModel):
    """
    An abstract mixin that provides semantic-search capabilities
    to a pydantic model using LanceDB
    """
    embedding: vector(1536)

    @classmethod
    @abstractmethod
    def uri(cls):
        """
        The URI of the LanceDB database to connect to
        """
        pass

    @classmethod
    @abstractmethod
    def table_name(cls):
        """
        The name of the LanceDB table to use
        """
        pass

    @classmethod
    def get_table(cls) -> lancedb.table.LanceTable:
        """
        Get the LanceDB table for this model
        """
        db = lancedb.connect(cls.uri())
        if cls.table_name() in db:
            return db.open_table(cls.table_name())
        return db.create_table(cls.table_name(), schema=cls.get_arrow_schema())

    @classmethod
    def get_arrow_schema(cls) -> pa.Schema:
        """
        Return the Arrow schema representation of this pydantic model
        """
        return pydantic_to_schema(cls)

    @classmethod
    @abstractmethod
    def generate_embedding(cls, batch: str | list[str]) -> list[np.ndarray]:
        """
        Generate an embedding for a batch of input
        """
        pass

    @classmethod
    def create_instances(cls, data: list[dict]) -> list[pydantic.BaseModel]:
        to_embed_name = cls._find_raw_data_column()  # could be None
        schema = cls.get_arrow_schema()
        if to_embed_name is not None:
            arrow_table = with_embeddings(cls.generate_embedding,
                                          pa.Table.from_pylist(data),
                                          column=to_embed_name)
            arrow_table = arrow_table.rename_columns(
                [name if name != "vector" else "embedding"
                 for name in arrow_table.schema.names])
            arrow_table = arrow_table.select(schema.names).cast(schema)
        else:
            arrow_table = pa.Table.from_pylist(data, schema=schema)
        cls.get_table().add(arrow_table)
        return [cls(**row) for row in arrow_table.to_pylist()]

    if PYDANTIC_VERSION < (2, 0):
        @classmethod
        def _find_raw_data_column(cls):
            for name, field in cls.schema()["properties"].items():
                if field.get("vector_input_column"):
                    return name
            return None
    else:
        @classmethod
        def _find_raw_data_column(cls):
            for name, field in cls.model_fields.items():
                if (field.json_schema_extra or {}).get("vector_input_column"):
                    return name
            return None

    @classmethod
    def search(cls, query: str, n: int = 3,
               filter: Optional[str] = None) -> list[pydantic.BaseModel]:
        """
        Search for the top n results matching the query
        """
        tbl = cls.get_table()
        embedding = cls.generate_embedding(query)[0]
        arrow_table = tbl.search(
            embedding, vector_column_name="embedding"
        ).where(filter).limit(n).to_arrow()
        return [cls(**row) for row in arrow_table.to_pylist()]

    @classmethod
    def reset(cls):
        """Delete the table and all of the data in it"""
        db = lancedb.connect(cls.uri())
        db.drop_table(cls.table_name())


def openai_embed_func(batch):
    import openai
    rs = openai.Embedding.create(input=batch, engine="text-embedding-ada-002")
    return [record["embedding"] for record in rs["data"]]


def lancedb_model(uri: str, table_name: str, embed_func=openai_embed_func):
    """
    Create a pydantic model that is backed by a LanceDB table.

    Parameters
    ----------
    uri : str
        The URI of the LanceDB database to use.
    table_name : str
        The name of the table to use.
    embed_func : callable
        A function that takes a list of strings and returns a list of
        embeddings. By default, this uses OpenAI's text-embedding-ada-002
    """
    return type(f'LanceDBMixin_{uuid4().hex}',
                (SearchableModel,),
                {
                    "uri": classmethod(lambda cls: uri),
                    "table_name": classmethod(lambda cls: table_name),
                    "generate_embedding": classmethod(lambda cls, batch: embed_func(batch))
                })
