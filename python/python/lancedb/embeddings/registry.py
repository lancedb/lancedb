# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import json
from typing import Dict, Optional

from .base import EmbeddingFunction, EmbeddingFunctionConfig


class EmbeddingFunctionRegistry:
    """
    This is a singleton class used to register embedding functions
    and fetch them by name. It also handles serializing and deserializing.
    You can implement your own embedding function by subclassing EmbeddingFunction
    or TextEmbeddingFunction and registering it with the registry.

    NOTE: Here TEXT is a type alias for Union[str, List[str], pa.Array,
          pa.ChunkedArray, np.ndarray]

    Examples
    --------
    >>> registry = EmbeddingFunctionRegistry.get_instance()
    >>> @registry.register("my-embedding-function")
    ... class MyEmbeddingFunction(EmbeddingFunction):
    ...     def ndims(self) -> int:
    ...         return 128
    ...
    ...     def compute_query_embeddings(self, query: str, *args, **kwargs):
    ...         return self.compute_source_embeddings(query, *args, **kwargs)
    ...
    ...     def compute_source_embeddings(self, texts, *args, **kwargs):
    ...         return [np.random.rand(self.ndims()) for _ in range(len(texts))]
    ...
    >>> registry.get("my-embedding-function")
    <class 'lancedb.embeddings.registry.MyEmbeddingFunction'>
    """

    @classmethod
    def get_instance(cls):
        return __REGISTRY__

    def __init__(self):
        self._functions = {}

    def register(self, alias: str = None):
        """
        This creates a decorator that can be used to register
        an EmbeddingFunction.

        Parameters
        ----------
        alias : Optional[str]
            a human friendly name for the embedding function. If not
            provided, the class name will be used.
        """

        # This is a decorator for a class that inherits from BaseModel
        # It adds the class to the registry
        def decorator(cls):
            if not issubclass(cls, EmbeddingFunction):
                raise TypeError("Must be a subclass of EmbeddingFunction")
            if cls.__name__ in self._functions:
                raise KeyError(f"{cls.__name__} was already registered")
            key = alias or cls.__name__
            self._functions[key] = cls
            cls.__embedding_function_registry_alias__ = alias
            return cls

        return decorator

    def reset(self):
        """
        Reset the registry to its initial state
        """
        self._functions = {}

    def get(self, name: str):
        """
        Fetch an embedding function class by name

        Parameters
        ----------
        name : str
            The name of the embedding function to fetch
            Either the alias or the class name if no alias was provided
            during registration
        """
        return self._functions[name]

    def parse_functions(
        self, metadata: Optional[Dict[bytes, bytes]]
    ) -> Dict[str, "EmbeddingFunctionConfig"]:
        """
        Parse the metadata from an arrow table and
        return a mapping of the vector column to the
        embedding function and source column

        Parameters
        ----------
        metadata : Optional[Dict[bytes, bytes]]
            The metadata from an arrow table. Note that
            the keys and values are bytes (pyarrow api)

        Returns
        -------
        functions : dict
            A mapping of vector column name to embedding function.
            An empty dict is returned if input is None or does not
            contain b"embedding_functions".
        """
        if metadata is None:
            return {}
        # Look at both bytes and string keys, since we might use either
        serialized = metadata.get(
            b"embedding_functions", metadata.get("embedding_functions")
        )
        if serialized is None:
            return {}
        raw_list = json.loads(serialized.decode("utf-8"))
        return {
            obj["vector_column"]: EmbeddingFunctionConfig(
                vector_column=obj["vector_column"],
                source_column=obj["source_column"],
                function=self.get(obj["name"])(**obj["model"]),
            )
            for obj in raw_list
        }

    def function_to_metadata(self, conf: "EmbeddingFunctionConfig"):
        """
        Convert the given embedding function and source / vector column configs
        into a config dictionary that can be serialized into arrow metadata
        """
        func = conf.function
        name = getattr(
            func, "__embedding_function_registry_alias__", func.__class__.__name__
        )
        json_data = func.safe_model_dump()
        return {
            "name": name,
            "model": json_data,
            "source_column": conf.source_column,
            "vector_column": conf.vector_column,
        }

    def get_table_metadata(self, func_list):
        """
        Convert a list of embedding functions and source / vector configs
        into a config dictionary that can be serialized into arrow metadata
        """
        if func_list is None or len(func_list) == 0:
            return None
        json_data = [self.function_to_metadata(func) for func in func_list]
        # Note that metadata dictionary values must be bytes
        # so we need to json dump then utf8 encode
        metadata = json.dumps(json_data, indent=2).encode("utf-8")
        return {"embedding_functions": metadata}


# Global instance
__REGISTRY__ = EmbeddingFunctionRegistry()


# @EmbeddingFunctionRegistry.get_instance().register(name) doesn't work in 3.8
def register(name):
    return __REGISTRY__.get_instance().register(name)


def get_registry() -> EmbeddingFunctionRegistry:
    """
    Utility function to get the global instance of the registry

    Returns
    -------
    EmbeddingFunctionRegistry
        The global registry instance

    Examples
    --------
    from lancedb.embeddings import get_registry

    registry = get_registry()
    openai = registry.get("openai").create()
    """
    return __REGISTRY__.get_instance()
