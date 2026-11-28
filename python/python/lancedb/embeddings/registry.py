# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import inspect
import json
from typing import Dict, Mapping, Optional, Set, Tuple, Type, Union

from pydantic import BaseModel, TypeAdapter

from .base import (
    EmbeddingFunction,
    EmbeddingFunctionConfig,
    UntrustedEmbeddingFunction,
)


def _embedding_column_pairs(
    metadata: Optional[Mapping[Union[bytes, str], Union[bytes, str]]],
) -> list[Tuple[str, str]]:
    """Return source/vector pairs without restoring embedding functions."""
    pairs = []
    seen = set()

    def add_pair(source_column: str, vector_column: str) -> None:
        pair = (source_column, vector_column)
        if pair not in seen:
            seen.add(pair)
            pairs.append(pair)

    if metadata is None:
        return pairs

    serialized = metadata.get(
        b"embedding_functions", metadata.get("embedding_functions")
    )
    if serialized is not None:
        if isinstance(serialized, bytes):
            serialized = serialized.decode("utf-8")
        for config in json.loads(serialized):
            add_pair(config["source_column"], config["vector_column"])

    serialized = metadata.get(
        b"lancedb::column_definitions", metadata.get("lancedb::column_definitions")
    )
    if serialized is not None:
        if isinstance(serialized, bytes):
            serialized = serialized.decode("utf-8")
        for column in json.loads(serialized):
            kind = column.get("kind")
            if not isinstance(kind, dict) or "Embedding" not in kind:
                continue
            embedding = kind["Embedding"]
            source_column = embedding["source_column"]
            vector_column = embedding.get("dest_column")
            if vector_column is None:
                vector_column = f"{source_column}_embedding"
            add_pair(source_column, vector_column)

    return pairs


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
        # Kept apart from the classes so subclasses don't inherit it: a subclass
        # can add settings that load or contact something new.
        self._safe_to_restore: Set[str] = set()
        self._variables = {}
        self._secret_variables: Set[str] = set()
        # Revision is a str, None for the default branch, or _ANY_REVISION.
        self._allowed_remote_code: Set[Tuple[str, object]] = set()
        self._allowed_endpoints: Set[str] = set()
        self._allowed_functions: Set[str] = set()
        self._trusted_configs: Set[Tuple[str, str]] = set()

    def register(self, alias: Optional[str] = None, *, safe_to_restore: bool = False):
        """
        This creates a decorator that can be used to register
        an EmbeddingFunction.

        Parameters
        ----------
        alias : Optional[str]
            a human friendly name for the embedding function. If not
            provided, the class name will be used.
        safe_to_restore : bool, default False
            Whether table metadata may configure this function without approval.
            Only set this if the class's
            [remote_code_model][lancedb.embeddings.EmbeddingFunction.remote_code_model],
            [loaded_models][lancedb.embeddings.EmbeddingFunction.loaded_models] and
            [endpoints][lancedb.embeddings.EmbeddingFunction.endpoints] report
            every model and URL its settings can make it load or contact. Those
            still need approval. Validators defined outside LanceDB also run
            when settings are checked, so a class that has them still needs
            `allow_function`. Otherwise, a function restored from table
            metadata only computes embeddings once the config is approved, see
            [allow_function][lancedb.embeddings.EmbeddingFunctionRegistry.allow_function].
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
            if safe_to_restore:
                self._safe_to_restore.add(key)
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
        self._safe_to_restore = set()

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

    def list_functions(self) -> list[str]:
        """List the names of registered embedding functions.

        Returns
        -------
        list[str]
            Registered aliases, or class names for functions registered without
            an alias, in registration order. Each name can be passed to
            :meth:`get`. The returned list is a copy; changing it does not
            change the registry.

        Examples
        --------
        >>> registry = get_registry()
        >>> "openai" in registry.list_functions()
        True
        """
        return list(self._functions)

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
        sensitive_keys = set(cls.sensitive_keys())
        # Sensitive values such as API keys stay unresolved until the config is
        # approved, so neither the checks nor their messages can contain them.
        public = cls._resolve_variables(
            {k: v for k, v in args.items() if k not in sensitive_keys},
            from_metadata=True,
        )
        inspector = _inspector(cls)
        approved = self._function_approved(name, args)
        if not approved and (inspector is None or name not in self._safe_to_restore):
            # Stop before inspecting the config, since validating it would run
            # code that hasn't been reviewed for side effects.
            if inspector is None:
                reason = (
                    "runs validators or initializers outside LanceDB while "
                    "validating its settings"
                )
            else:
                reason = "is not registered with safe_to_restore=True"
            func = UntrustedEmbeddingFunction(
                function_name=name,
                reasons=(
                    f"{reason}, so its settings need approval. If you trust "
                    "any settings for it from table metadata, call "
                    f"get_registry().allow_function({name!r})",
                ),
            )
            func._original_args = args
            return func
        if inspector is None:
            # Approved for any settings, and its settings can't be inspected.
            func = cls(**public, **self._resolve_secrets(cls, args))
            func._original_args = args
            return func
        # Unresolved sensitive values stand in for the real ones, so required
        # fields such as API keys validate without exposing secrets.
        probe = inspector.model_validate({**args, **public})

        reasons = []
        model = probe.remote_code_model()
        if model is not None:
            allowed = self._remote_code_allowed(*model)
            explicit = "trust_remote_code" in public
            if explicit:
                requested = TypeAdapter(bool).validate_python(
                    public["trust_remote_code"]
                )
            else:
                requested = cls._legacy_trust_remote_code_default()
            # Always set the flag, since a subclass may default it to True.
            public["trust_remote_code"] = requested and allowed
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
            f"loads model '{model_name}' at revision {revision!r}, whose files "
            "can run code. If you trust it, call "
            f"get_registry().allow_remote_code({model_name!r}, "
            f"revision={revision!r})"
            for model_name, revision in probe.loaded_models()
            if not self._remote_code_allowed(model_name, revision)
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
            func = cls(**public, **self._resolve_secrets(cls, args))
        func._original_args = args
        return func

    def _function_approved(self, name: str, args: dict) -> bool:
        return (
            name in self._allowed_functions
            or (name, _canonical_config(args)) in self._trusted_configs
        )

    @staticmethod
    def _resolve_secrets(cls: Type[EmbeddingFunction], args: dict) -> dict:
        sensitive_keys = set(cls.sensitive_keys())
        return cls._resolve_variables(
            {k: v for k, v in args.items() if k in sensitive_keys}
        )

    def allow_remote_code(self, model: str, revision: Optional[str] = None) -> None:
        """
        Trust the code in a model repository when an embedding function that
        loads it is restored from table metadata.

        Table metadata cannot enable `trust_remote_code` by itself, since anyone
        who can write to the table could then run code in this process. For the
        same reason, it cannot pick a model for functions whose model files can
        run code when loaded, such as pickled checkpoints, other than the
        function's default model. Embedding functions created in this process
        are allowed to load their models automatically, and to run their code if
        created with `trust_remote_code=True`.

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

    def allow_function(self, name: str) -> None:
        """
        Trust any settings from table metadata for an embedding function that
        isn't registered with `safe_to_restore=True`, such as your own
        embedding functions.

        LanceDB can't tell what such a function's settings make it load or
        contact, so when it is restored from table metadata it only computes
        embeddings for configs created in this process, unless you call this.
        Models and endpoints the function reports still need approval with
        [allow_remote_code][lancedb.embeddings.EmbeddingFunctionRegistry.allow_remote_code]
        and
        [allow_endpoint][lancedb.embeddings.EmbeddingFunctionRegistry.allow_endpoint].

        Parameters
        ----------
        name : str
            The name the function was registered under, such as `"bedrock-text"`.
        """
        self._allowed_functions.add(name)

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
        self._allowed_remote_code.update(func.loaded_models())
        for url in func.endpoints():
            self.allow_endpoint(url)
        name = next(
            (k for k, cls in self._functions.items() if cls is type(func)), None
        )
        if name is not None and name not in self._safe_to_restore:
            self._trusted_configs.add((name, _canonical_config(func._original_args)))

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

    def set_var(self, name: str, value: str, *, secret: bool = True) -> None:
        """
        Set a variable. These can be accessed in embedding configuration using
        the syntax `$var:variable_name`. If they are not set, an error will be
        thrown letting you know which variable is missing. If you want to supply
        a default value, you can add an additional part in the configuration
        like so: `$var:variable_name:default_value`. Default values can be
        used for runtime configurations that are not sensitive, such as
        whether to use a GPU for inference.

        The name must not contain a colon. Default values can contain colons.

        Parameters
        ----------
        name : str
            The variable name.
        value : str
            The value to substitute.
        secret : bool, default True
            Whether the value is a secret, such as an API key. Secret variables
            can only be used in an embedding function's sensitive fields, which
            send them only as credentials. Anyone who can write a table chooses
            the variable names in its embedding config, so a secret in any other
            field, such as a URL, could be shown to them in an error message or
            sent to a server they pick. Pass `secret=False` for settings such as
            the device to run on.
        """
        if ":" in name:
            raise ValueError("Variable names cannot contain colons")
        self._variables[name] = value
        if secret:
            self._secret_variables.add(name)
        else:
            self._secret_variables.discard(name)

    def get_var(self, name: str) -> str:
        """
        Get a variable.
        """
        return self._variables[name]


_ANY_REVISION = object()


def _normalize_url(url: str) -> str:
    return url.rstrip("/")


def _inspector(cls: Type[EmbeddingFunction]) -> Optional[Type[EmbeddingFunction]]:
    """
    A subclass of `cls` whose validation binds settings as `cls(**values)`
    would, so aliases and extra settings resolve the same way, but without
    running `cls.__init__` or `cls.model_post_init`, either of which may load
    the configured model. Returns None if validation would run other code that
    could have side effects, such as validators defined outside LanceDB.
    """
    if cls not in _inspectors:

        class inspector(cls):
            __init__ = BaseModel.__init__
            model_post_init = BaseModel.model_post_init

        safe = _validation_is_side_effect_free(
            inspector.__pydantic_core_schema__, inspector
        )
        _inspectors[cls] = inspector if safe else None
    return _inspectors[cls]


_inspectors: Dict[type, Optional[type]] = {}
_TRUSTED_VALIDATOR_MODULES = ("lancedb", "pydantic", "pydantic_core", "builtins")


def _validation_is_side_effect_free(schema, inspector: type) -> bool:
    if isinstance(schema, list):
        return all(_validation_is_side_effect_free(s, inspector) for s in schema)
    if not isinstance(schema, dict):
        return True
    kind = schema.get("type")
    if kind == "model" and schema["cls"] is not inspector:
        nested = schema["cls"]
        if nested.__pydantic_custom_init__ or nested.__pydantic_post_init__:
            return False
    if kind == "dataclass" and schema.get("post_init"):
        return False
    functions = [schema.get("default_factory")]
    if isinstance(schema.get("function"), dict):
        functions.append(schema["function"].get("function"))
    if not all(f is None or _is_trusted_function(f) for f in functions):
        return False
    # Serializers and JSON schema hooks don't run during validation.
    return all(
        _validation_is_side_effect_free(value, inspector)
        for key, value in schema.items()
        if key not in ("serialization", "metadata")
    )


def _is_trusted_function(func) -> bool:
    func = inspect.unwrap(getattr(func, "__func__", func))
    module = getattr(func, "__module__", None) or ""
    return module.split(".")[0] in _TRUSTED_VALIDATOR_MODULES


def _canonical_config(args: dict) -> str:
    return json.dumps(args, sort_keys=True, default=repr)


# Global instance
__REGISTRY__ = EmbeddingFunctionRegistry()


# @EmbeddingFunctionRegistry.get_instance().register(name) doesn't work in 3.8
def register(name, *, safe_to_restore: bool = False):
    return __REGISTRY__.get_instance().register(name, safe_to_restore=safe_to_restore)


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
