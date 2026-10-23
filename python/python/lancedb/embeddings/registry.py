# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import json
from typing import Dict, Optional, Set, Tuple, Type

from pydantic import TypeAdapter

from .base import (
    EmbeddingFunction,
    EmbeddingFunctionConfig,
    UntrustedEmbeddingFunction,
)


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
        self._variables = {}
        # Revision is a str, None for the default branch, or _ANY_REVISION.
        self._allowed_remote_code: Set[Tuple[str, object]] = set()
        self._allowed_endpoints: Set[str] = set()

    def register(self, alias: Optional[str] = None):
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
            key = alias or cls.__name__
            if key in self._functions and (
                not alias or self._functions[key] is not cls
            ):
                raise KeyError(f"{key} was already registered")
            self._functions[key] = cls
            # Store the name the class is registered under, so the table
            # metadata written for it can be looked up again when reading.
            cls.__embedding_function_registry_alias__ = key
            return cls

        return decorator

    def reset(self):
        """
        Reset the registry to its initial state
        """
        self._functions = {}

    def get(self, name: str) -> Type[EmbeddingFunction]:
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
                function=self._restore(obj["name"], obj["model"]),
            )
            for obj in raw_list
        }

    def _restore(self, name: str, args: dict) -> EmbeddingFunction:
        """
        Create an embedding function from table metadata. Anyone who can write
        the table controls `args`, so settings that run model code or send data
        elsewhere must already be trusted by this process.
        """
        cls = self.get(name)
        resolved = cls._resolve_variables(args)
        # Inspect the config without running __init__, which may load a model.
        probe = cls.model_construct(**resolved)

        reasons = []
        model = probe.remote_code_model()
        if model is not None:
            allowed = self._remote_code_allowed(*model)
            explicit = "trust_remote_code" in resolved
            if explicit:
                requested = TypeAdapter(bool).validate_python(
                    resolved["trust_remote_code"]
                )
            else:
                requested = cls._legacy_trust_remote_code_default()
            # Always set the flag, since a subclass may default it to True.
            resolved["trust_remote_code"] = requested and allowed
            # A legacy table that omits the flag is downgraded rather than
            # blocked: most sentence-transformers models don't need remote code,
            # and those tables relied on the old True default without asking.
            if requested and not allowed and explicit:
                model_name, revision = model
                reasons.append(
                    f"runs code from model '{model_name}' at revision "
                    f"{revision!r}. If you trust it, call "
                    f"get_registry().allow_remote_code({model_name!r}, "
                    f"revision={revision!r})"
                )
        reasons.extend(
            f"sends data to endpoint '{url}'. If you trust it, call "
            f"get_registry().allow_endpoint({url!r})"
            for url in probe.endpoints()
            if _normalize_url(url) not in self._allowed_endpoints
        )

        if reasons:
            func = UntrustedEmbeddingFunction(
                function_name=name, reasons=tuple(reasons)
            )
        else:
            func = cls(**resolved)
        func._original_args = args
        return func

    def allow_remote_code(self, model: str, revision: Optional[str] = None) -> None:
        """
        Trust the code in a model repository when an embedding function that
        loads it is restored from table metadata.

        Table metadata cannot enable `trust_remote_code` by itself, since anyone
        who can write to the table could then run code in this process. Embedding
        functions created in this process with `trust_remote_code=True` are
        allowed automatically.

        Parameters
        ----------
        model : str
            The model name, such as `"nomic-ai/nomic-embed-text-v1.5"`.
        revision : Optional[str]
            The revision the table's embedding function must pin. Pinning a
            commit hash is recommended, since the code at a branch or tag can
            change after you review it. If None, any revision is trusted,
            including pull request refs that anyone can push to the repository.
        """
        self._allowed_remote_code.add(
            (model, _ANY_REVISION if revision is None else revision)
        )

    def allow_endpoint(self, url: str) -> None:
        """
        Trust a custom endpoint, such as an OpenAI `base_url` or Ollama `host`,
        when an embedding function that uses it is restored from table metadata.

        Table metadata cannot pick an endpoint by itself, since it could send
        your data and API keys to a server you don't control. Embedding functions
        created in this process are allowed to use their endpoints automatically.

        Parameters
        ----------
        url : str
            The endpoint URL. Trailing slashes are ignored.
        """
        self._allowed_endpoints.add(_normalize_url(url))

    def _remote_code_allowed(self, model: str, revision: Optional[str]) -> bool:
        return (model, _ANY_REVISION) in self._allowed_remote_code or (
            model,
            revision,
        ) in self._allowed_remote_code

    def _trust(self, func: EmbeddingFunction) -> None:
        model = func.remote_code_model()
        if model is not None and getattr(func, "trust_remote_code", False):
            # Only the revision this code selected, where None means the default
            # branch. Other revisions, such as PR refs, may hold other code.
            self._allowed_remote_code.add(model)
        for url in func.endpoints():
            self.allow_endpoint(url)

    def function_to_metadata(self, conf: "EmbeddingFunctionConfig"):
        """
        Convert the given embedding function and source / vector column configs
        into a config dictionary that can be serialized into arrow metadata
        """
        func = conf.function
        if isinstance(func, UntrustedEmbeddingFunction):
            name = func.function_name
        else:
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

    def set_var(self, name: str, value: str) -> None:
        """
        Set a variable. These can be accessed in embedding configuration using
        the syntax `$var:variable_name`. If they are not set, an error will be
        thrown letting you know which variable is missing. If you want to supply
        a default value, you can add an additional part in the configuration
        like so: `$var:variable_name:default_value`. Default values can be
        used for runtime configurations that are not sensitive, such as
        whether to use a GPU for inference.

        The name must not contain a colon. Default values can contain colons.
        """
        if ":" in name:
            raise ValueError("Variable names cannot contain colons")
        self._variables[name] = value

    def get_var(self, name: str) -> str:
        """
        Get a variable.
        """
        return self._variables[name]


_ANY_REVISION = object()


def _normalize_url(url: str) -> str:
    return url.rstrip("/")


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
