# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

from abc import ABC, abstractmethod
import copy
from typing import List, Optional, Tuple, Union

from lancedb.util import add_note
import numpy as np
import pyarrow as pa
from pydantic import BaseModel, Field, PrivateAttr

from .utils import TEXT, retry_with_exponential_backoff


class UntrustedEmbeddingConfigError(PermissionError):
    """
    Raised when using an embedding function restored from table metadata whose
    settings this process has not approved.

    See
    [allow_remote_code][lancedb.embeddings.EmbeddingFunctionRegistry.allow_remote_code]
    and
    [allow_endpoint][lancedb.embeddings.EmbeddingFunctionRegistry.allow_endpoint].
    """


class EmbeddingFunction(BaseModel, ABC):
    """
    An ABC for embedding functions.

    All concrete embedding functions must implement the following methods:
    1. compute_query_embeddings() which takes a query and returns a list of embeddings
    2. compute_source_embeddings() which returns a list of embeddings for
       the source column
    For text data, the two will be the same. For multi-modal data, the source column
    might be images and the vector column might be text.
    3. ndims() which returns the number of dimensions of the vector column
    """

    # Maximum number of retries for transient errors (rate limits, 5xx responses,
    # connection errors, ...), with exponential backoff capped at 60 seconds between
    # attempts. Permanent errors (HTTP 4xx other than 408/409/429) are never retried.
    # Setting 0 disables retries.
    max_retries: int = 7
    _ndims: int = PrivateAttr()
    _original_args: dict = PrivateAttr()

    @classmethod
    def create(cls, **kwargs):
        """
        Create an instance of the embedding function
        """
        from .registry import EmbeddingFunctionRegistry

        resolved_kwargs = cls.__resolveVariables(kwargs)
        instance = cls(**resolved_kwargs)
        instance._original_args = dict(kwargs)
        if (
            instance.remote_code_model() is not None
            and "trust_remote_code" in cls.model_fields
        ):
            # Record the flag even when left at its default, so a missing flag
            # in table metadata only ever means a table written before this.
            instance._original_args.setdefault(
                "trust_remote_code", instance.trust_remote_code
            )
        # The caller wrote this config, so this process trusts it. Without this,
        # a table created with it could not be reopened in the same process.
        EmbeddingFunctionRegistry.get_instance()._trust(instance)
        return instance

    @classmethod
    def _resolve_variables(cls, args: dict, *, from_metadata: bool = False) -> dict:
        return cls.__resolveVariables(args, from_metadata=from_metadata)

    @classmethod
    def __resolveVariables(cls, args: dict, *, from_metadata: bool = False) -> dict:
        """
        Resolve variables in the args.

        Secret variables only resolve in sensitive fields. When the args come
        from table metadata, a secret variable in any other field is treated as
        unset, so the result is the same whether or not the variable exists.
        """
        from .registry import EmbeddingFunctionRegistry

        new_args = copy.deepcopy(args)

        registry = EmbeddingFunctionRegistry.get_instance()
        sensitive_keys = cls.sensitive_keys()
        for k, v in new_args.items():
            sensitive = k in sensitive_keys
            if isinstance(v, str) and not v.startswith("$var:") and sensitive:
                exc = ValueError(
                    f"Sensitive key '{k}' cannot be set to a hardcoded value"
                )
                add_note(exc, "Help: Use $var: to set sensitive keys to variables")
                raise exc

            if not (isinstance(v, str) and v.startswith("$var:")):
                continue
            name, *default = v[5:].split(":", maxsplit=1)
            visible = name in registry._variables
            if visible and not sensitive and name in registry._secret_variables:
                if not from_metadata:
                    exc = ValueError(
                        f"Variable '{name}' is secret, so it can only be used in "
                        f"sensitive fields such as API keys, not '{k}'"
                    )
                    add_note(
                        exc,
                        "Help: If it isn't a secret, set it with "
                        f"`registry.set_var({name!r}, value, secret=False)`.",
                    )
                    raise exc
                visible = False
            if visible:
                new_args[k] = registry.get_var(name)
            elif default:
                new_args[k] = default[0]
            else:
                exc = ValueError("Variable '{}' not found in registry".format(name))
                add_note(
                    exc,
                    "Help: Variables are reset in new Python sessions. "
                    "Use `registry.set_var` to set variables.",
                )
                raise exc
        return new_args

    @staticmethod
    def sensitive_keys() -> List[str]:
        """
        Return a list of keys that are sensitive and should not be allowed
        to be set to hardcoded values in the config. For example, API keys.
        """
        return []

    def remote_code_model(self) -> Optional[Tuple[str, Optional[str]]]:
        """
        Return the model whose repository code this function runs when its
        `trust_remote_code` field is enabled, as a `(name, revision)` pair.

        Return None if the function never runs code from a model repository.
        When the function is restored from table metadata, `trust_remote_code`
        is only enabled for models allowed with
        [allow_remote_code][lancedb.embeddings.EmbeddingFunctionRegistry.allow_remote_code].
        """
        return None

    def loaded_models(self) -> List[Tuple[str, Optional[str]]]:
        """
        Return the models this function loads whose files can run code when
        loaded, such as pickled checkpoints, as `(name, revision)` pairs.

        Leave out the function's default model, which its author chose. When
        the function is restored from table metadata, each of these must be
        allowed with
        [allow_remote_code][lancedb.embeddings.EmbeddingFunctionRegistry.allow_remote_code].
        """
        return []

    def _uses_default(self, *fields: str) -> bool:
        return all(
            getattr(self, f) == type(self).model_fields[f].default for f in fields
        )

    @classmethod
    def _legacy_trust_remote_code_default(cls) -> bool:
        """
        The `trust_remote_code` to use for table metadata that omits it. Only
        tables written before the flag was always recorded omit it, so this is
        the default from that time.
        """
        field = cls.model_fields.get("trust_remote_code")
        return bool(field.default) if field is not None else False

    def endpoints(self) -> List[str]:
        """
        Return the non-default URLs this function sends data or credentials to.

        When the function is restored from table metadata, each of these must be
        allowed with
        [allow_endpoint][lancedb.embeddings.EmbeddingFunctionRegistry.allow_endpoint].
        """
        return []

    @abstractmethod
    def compute_query_embeddings(self, *args, **kwargs) -> list[Union[np.array, None]]:
        """
        Compute the embeddings for a given user query

        Returns
        -------
        A list of embeddings for each input. The embedding of each input can be None
        when the embedding is not valid.
        """
        pass

    @abstractmethod
    def compute_source_embeddings(self, *args, **kwargs) -> list[Union[np.array, None]]:
        """Compute the embeddings for the source column in the database

        Returns
        -------
        A list of embeddings for each input. The embedding of each input can be None
        when the embedding is not valid.
        """
        pass

    def compute_query_embeddings_with_retry(
        self, *args, **kwargs
    ) -> list[Union[np.array, None]]:
        """Compute the embeddings for a given user query with retries

        Returns
        -------
        A list of embeddings for each input. The embedding of each input can be None
        when the embedding is not valid.
        """
        return retry_with_exponential_backoff(
            self.compute_query_embeddings, max_retries=self.max_retries
        )(
            *args,
            **kwargs,
        )

    def compute_source_embeddings_with_retry(
        self, *args, **kwargs
    ) -> list[Union[np.array, None]]:
        """Compute the embeddings for the source column in the database with retries.

        Returns
        -------
        A list of embeddings for each input. The embedding of each input can be None
        when the embedding is not valid.
        """
        return retry_with_exponential_backoff(
            self.compute_source_embeddings, max_retries=self.max_retries
        )(*args, **kwargs)

    def sanitize_input(self, texts: TEXT) -> Union[List[str], np.ndarray]:
        """
        Sanitize the input to the embedding function.
        """
        if isinstance(texts, str):
            texts = [texts]
        elif isinstance(texts, pa.Array):
            texts = texts.to_pylist()
        elif isinstance(texts, pa.ChunkedArray):
            texts = texts.combine_chunks().to_pylist()
        return texts

    def safe_model_dump(self):
        if not hasattr(self, "_original_args"):
            raise ValueError(
                "EmbeddingFunction was not created with EmbeddingFunction.create()"
            )
        return self._original_args

    @abstractmethod
    def ndims(self) -> int:
        """
        Return the dimensions of the vector column
        """
        pass

    def SourceField(self, **kwargs):
        """
        Creates a pydantic Field that can automatically annotate
        the source column for this embedding function
        """
        json_schema_extra = dict(kwargs.pop("json_schema_extra", None) or {})
        json_schema_extra["source_column_for"] = self
        return Field(json_schema_extra=json_schema_extra, **kwargs)

    def VectorField(self, **kwargs):
        """
        Creates a pydantic Field that can automatically annotate
        the target vector column for this embedding function.

        The field can be annotated as ``list[float]`` for compatibility with
        static type checkers. LanceDB will infer the fixed vector dimension from
        this embedding function.
        """
        json_schema_extra = dict(kwargs.pop("json_schema_extra", None) or {})
        json_schema_extra["vector_column_for"] = self
        return Field(json_schema_extra=json_schema_extra, **kwargs)

    def __eq__(self, __value: object) -> bool:
        if not hasattr(__value, "__dict__"):
            return False
        return vars(self) == vars(__value)

    def __hash__(self) -> int:
        return hash(frozenset(vars(self).items()))


class EmbeddingFunctionConfig(BaseModel):
    """
    This model encapsulates the configuration for a embedding function
    in a lancedb table. It holds the embedding function, the source column,
    and the vector column
    """

    vector_column: str
    source_column: str
    function: EmbeddingFunction


class TextEmbeddingFunction(EmbeddingFunction):
    """
    A callable ABC for embedding functions that take text as input
    """

    def compute_query_embeddings(
        self, query: str, *args, **kwargs
    ) -> list[Union[np.array, None]]:
        return self.compute_source_embeddings(query, *args, **kwargs)

    def compute_source_embeddings(
        self, texts: TEXT, *args, **kwargs
    ) -> list[Union[np.array, None]]:
        texts = self.sanitize_input(texts)
        return self.generate_embeddings(texts)

    @abstractmethod
    def generate_embeddings(
        self, texts: Union[List[str], np.ndarray], *args, **kwargs
    ) -> list[Union[np.array, None]]:
        """Generate the embeddings for the given texts"""
        pass


class UntrustedEmbeddingFunction(EmbeddingFunction):
    """
    Stands in for an embedding function restored from table metadata whose
    settings this process has not approved.

    The table stays usable for anything that doesn't compute embeddings, while
    computing embeddings raises
    [UntrustedEmbeddingConfigError][lancedb.embeddings.UntrustedEmbeddingConfigError].
    The real function is never constructed, since some load their model in
    `__init__`.
    """

    function_name: str
    reasons: Tuple[str, ...]

    def _error(self) -> UntrustedEmbeddingConfigError:
        details = "\n".join(f"  - {reason}" for reason in self.reasons)
        return UntrustedEmbeddingConfigError(
            f"Embedding function '{self.function_name}' was restored from table "
            "metadata with settings this process has not approved:\n" + details
        )

    def compute_query_embeddings(self, *args, **kwargs):
        raise self._error()

    def compute_source_embeddings(self, *args, **kwargs):
        raise self._error()

    def ndims(self) -> int:
        raise self._error()
