#  Copyright (c) 2023. LanceDB Developers
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
import json
from abc import ABC, abstractmethod
from typing import List, Union

import numpy as np
from cachetools import cached
from pydantic import BaseModel


class EmbeddingFunctionRegistry:
    """
    This is a singleton class is used to register embedding functions
    and fetch them by name. It also handles serializing and deserializing
    """

    def __init__(self):
        self._functions = {}

    def register(self, alias: str = None):
        """
        This creates a decorator that can be used to register
        an EmbeddingFunctionModel.

        Parameters
        ----------
        alias : str, optional
            The alias is used to identify the function when
            serializing/deserializing. If no alias is supplied,
            then the class name is used.
        """

        # This is a decorator for a class that inherits from BaseModel
        # It adds the class to the registry
        def decorator(cls):
            if not issubclass(cls, EmbeddingFunctionModel):
                raise TypeError("Must be a subclass of EmbeddingFunctionModel")
            cls.__function_alias__ = alias or cls.__name__
            self._functions[alias or cls.__name__] = cls
            return cls

        return decorator

    def load(self, name: str):
        """
        Fetch an embedding function class by name
        """
        return self._functions[name]

    def parse_functions(self, metadata: dict):
        """
        Parse the metadata from an arrow table and
        return a mapping of the vector column to the
        embedding function and source column

        Parameters
        ----------
        metadata : dict
            The metadata from an arrow table. Note that
            the keys and values are bytes
        """
        serialized = metadata[b"embedding_functions"]
        raw_list = json.loads(serialized.decode("utf-8"))
        functions = {}
        for obj in raw_list:
            model = self.load(obj["schema"]["alias"])
            functions[obj["vector_column"]] = {
                "source_column": obj["source_column"],
                "embedding_function": model(**obj["model"]),
            }
        return functions

    def function_to_metadata(self, func, source_column, vector_column):
        """
        Convert the given embedding function and source / vector column configs
        into a config dictionary that can be serialized into arrow metadata
        """
        schema = func.model_json_schema()
        schema["alias"] = getattr(func, "__function_alias__", func.__class__.__name__)
        json_data = func.model_dump()
        return {
            "source_column": source_column,
            "vector_column": vector_column,
            "schema": schema,
            "model": json_data,
        }

    def get_table_metadata(self, func_list):
        """
        Convert a list of embedding functions and source / vector column configs
        into a config dictionary that can be serialized into arrow metadata
        """
        json_data = [
            self.function_to_metadata(
                self._maybe_to_function(func["function"]),
                func["source_column"],
                func["vector_column"],
            )
            for func in func_list
        ]
        # Note that metadata dictionary values must be bytes so we need to json dump then utf8 encode
        metadata = json.dumps(json_data, indent=2).encode("utf-8")
        return {"embedding_functions": metadata}

    def _maybe_to_function(self, func):
        if isinstance(func, EmbeddingFunctionModel):
            return func
        elif isinstance(func, str):
            return self.load(func)()
        else:
            raise ValueError(f"Invalid function type: {func}")


REGISTRY = EmbeddingFunctionRegistry()


class EmbeddingFunctionModel(BaseModel, ABC):
    """
    A callable ABC for embedding functions
    """

    @abstractmethod
    def __call__(self, *args, **kwargs):
        pass


@REGISTRY.register("sentence-transformers")
class SentenceTransformerEmbeddingFunction(EmbeddingFunctionModel):
    name: str = "all-MiniLM-L6-v2"
    device: str = "cpu"
    normalize: bool = False

    @property
    def embedding_model(self):
        """
        Get the sentence-transformers embedding model specified by the
        name and device. This is cached so that the model is only loaded
        once per process.
        """
        return self.__class__.get_embedding_model(self.name, self.device)

    def __call__(self, texts: Union[str, List[str]]) -> List[np.array]:
        """
        Get the embeddings for the given texts

        Parameters
        ----------
        texts: str or list[str]
            The texts to embed
        """
        if isinstance(texts, str):
            texts = [texts]
        return self.embedding_model.encode(
            list(texts),
            convert_to_numpy=True,
            normalize_embeddings=self.normalize,
        ).tolist()

    @classmethod
    @cached(cache={})
    def get_embedding_model(cls, name, device):
        """
        Get the sentence-transformers embedding model specified by the
        name and device. This is cached so that the model is only loaded
        once per process.

        Parameters
        ----------
        name : str
            The name of the model to load
        device : str
            The device to load the model on

        TODO: use lru_cache instead with a reasonable/configurable? maxsize
        """
        try:
            from sentence_transformers import SentenceTransformer

            return SentenceTransformer(name, device=device)
        except ImportError:
            raise ValueError("Please install sentence_transformers")
