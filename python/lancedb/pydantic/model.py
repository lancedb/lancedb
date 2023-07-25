from abc import ABC, abstractmethod
from contextlib import ContextDecorator, contextmanager
from typing import Optional, Union
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

    def __init__(self, **kwargs):
        to_embed_name = self.__class__._find_raw_data_column()  # could be None
        embedding_col = self.__class__._find_embedding_column()
        if kwargs.get(embedding_col) is None and to_embed_name is not None:
            embedding = self.generate_embeddings([kwargs[to_embed_name]])[0]
            kwargs[embedding_col] = embedding
        super().__init__()

    @classmethod
    @abstractmethod
    def generate_embeddings(cls, batch: str | list[str]) -> list[np.ndarray]:
        """
        Generate an embedding for a batch of input
        """
        pass

    @classmethod
    @abstractmethod
    def get_table(cls) -> lancedb.table.LanceTable:
        pass

    @classmethod
    def get_arrow_schema(cls) -> pa.Schema:
        """
        Return the Arrow schema representation of this pydantic model
        """
        return pydantic_to_schema(cls)

    if PYDANTIC_VERSION < (2, 0):
        @classmethod
        def _find_raw_data_column(cls):
            for name, field in cls.schema()["properties"].items():
                if field.get("embedding_source"):
                    return name
            return None

        @classmethod
        def _find_embedding_column(cls):
            for name, field in cls.schema()["properties"].items():
                if (field.get("embedding") or
                        name.lower() in ["embedding", "vector"]):
                    return name
            return None
    else:
        @classmethod
        def _find_raw_data_column(cls):
            for name, field in cls.model_fields.items():
                if (field.json_schema_extra or {}).get("embedding_source"):
                    return name
            return None

        @classmethod
        def _find_embedding_column(cls):
            for name, field in cls.model_fields.items():
                if ((field.json_schema_extra or {}).get("embedding") or
                        name.lower() in ["embedding", "vector"]):
                    return name
            return None

    @classmethod
    def search(cls, query: str, n: int = 3,
               filter: Optional[str] = None,
               column: Optional[str] = None) -> list[pydantic.BaseModel]:
        """
        Search for the top n results matching the query

        Parameters
        ----------
        query: str
            To be embedded
        n: int
            Number of results to return
        filter: str, default None
            Post filter to execute after vector search
        column: str, default None
            The name of the embedding column to search over
            If None, then check the fields that are marked `embedding=True`
            or named "embedding" or "vector"
        """
        column = column or cls._find_embedding_column()
        tbl = cls.get_table()
        embedding = cls.generate_embeddings(query)[0]
        arrow_table = tbl.search(
            embedding, column=column
        ).where(filter).limit(n).to_arrow()
        return [cls(**row) for row in arrow_table.to_pylist()]

    @classmethod
    def reset(cls):
        """Delete the table and all of the data in it"""
        cls.get_table().drop()

    @classmethod
    @contextmanager
    def session(cls):
        yield Session(cls.get_table())


class Session:

    def __init__(self, table: lancedb.table.LanceTable):
        self.table = table
        self._buffer = []

    def commit(self):
        if len(self._buffer) == 0:
            return
        input_schema = pydantic_to_schema(type(self._buffer[0]))
        arrow_table = pa.Table.from_pylist(
            [dict(m) for m in self._buffer],
            schema=input_schema)
        self.table.add(
            arrow_table
            .select(self.table.schema.names)
            .cast(self.table.schema)
        )

    def add(self, data: Union[pydantic.BaseModel, list[pydantic.BaseModel]]):
        if isinstance(data, pydantic.BaseModel):
            data = [data]
        self._buffer.extend(data)

    def __exit__(self, *exc):
        self.commit()
        self._buffer.clear()


def openai_embed_func(batch):
    import openai
    rs = openai.Embedding.create(input=batch, engine="text-embedding-ada-002")
    return [record["embedding"] for record in rs["data"]]


def lancedb_model(uri: str, table_name: str,
                  embed_func=openai_embed_func):
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
                (SearchableModel, pydantic.BaseModel),
                {
                    "uri": classmethod(lambda cls: uri),
                    "table_name": classmethod(lambda cls: table_name),
                    "generate_embedding": classmethod(lambda cls, batch: embed_func(batch))
                })
