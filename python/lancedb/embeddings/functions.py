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
import concurrent.futures
import importlib
import io
import json
import os
import socket
import urllib.error
import urllib.parse as urlparse
import urllib.request
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Union

import numpy as np
import pyarrow as pa
from cachetools import cached
from pydantic import BaseModel, Field, PrivateAttr
from tqdm import tqdm


class EmbeddingFunctionRegistry:
    """
    This is a singleton class used to register embedding functions
    and fetch them by name. It also handles serializing and deserializing.
    You can implement your own embedding function by subclassing EmbeddingFunction
    or TextEmbeddingFunction and registering it with the registry.

    Examples
    --------
    >>> registry = EmbeddingFunctionRegistry.get_instance()
    >>> @registry.register("my-embedding-function")
    ... class MyEmbeddingFunction(EmbeddingFunction):
    ...     def ndims(self) -> int:
    ...         return 128
    ...
    ...     def compute_query_embeddings(self, query: str, *args, **kwargs) -> List[np.array]:
    ...         return self.compute_source_embeddings(query, *args, **kwargs)
    ...
    ...     def compute_source_embeddings(self, texts: TEXT, *args, **kwargs) -> List[np.array]:
    ...         return [np.random.rand(self.ndims()) for _ in range(len(texts))]
    ...
    >>> registry.get("my-embedding-function")
    <class 'lancedb.embeddings.functions.MyEmbeddingFunction'>
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
        if metadata is None or b"embedding_functions" not in metadata:
            return {}
        serialized = metadata[b"embedding_functions"]
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


TEXT = Union[str, List[str], pa.Array, pa.ChunkedArray, np.ndarray]
IMAGES = Union[
    str, bytes, List[str], List[bytes], pa.Array, pa.ChunkedArray, np.ndarray
]


class EmbeddingFunction(BaseModel, ABC):
    """
    An ABC for embedding functions.

    All concrete embedding functions must implement the following:
    1. compute_query_embeddings() which takes a query and returns a list of embeddings
    2. get_source_embeddings() which returns a list of embeddings for the source column
    For text data, the two will be the same. For multi-modal data, the source column
    might be images and the vector column might be text.
    3. ndims method which returns the number of dimensions of the vector column
    """

    _ndims: int = PrivateAttr()

    @classmethod
    def create(cls, **kwargs):
        """
        Create an instance of the embedding function
        """
        return cls(**kwargs)

    @abstractmethod
    def compute_query_embeddings(self, *args, **kwargs) -> List[np.array]:
        """
        Compute the embeddings for a given user query
        """
        pass

    @abstractmethod
    def compute_source_embeddings(self, *args, **kwargs) -> List[np.array]:
        """
        Compute the embeddings for the source column in the database
        """
        pass

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

    @classmethod
    def safe_import(cls, module: str, mitigation=None):
        """
        Import the specified module. If the module is not installed,
        raise an ImportError with a helpful message.

        Parameters
        ----------
        module : str
            The name of the module to import
        mitigation : Optional[str]
            The package(s) to install to mitigate the error.
            If not provided then the module name will be used.
        """
        try:
            return importlib.import_module(module)
        except ImportError:
            raise ImportError(f"Please install {mitigation or module}")

    def safe_model_dump(self):
        from ..pydantic import PYDANTIC_VERSION

        if PYDANTIC_VERSION.major < 2:
            return dict(self)
        return self.model_dump()

    @abstractmethod
    def ndims(self):
        """
        Return the dimensions of the vector column
        """
        pass

    def SourceField(self, **kwargs):
        """
        Creates a pydantic Field that can automatically annotate
        the source column for this embedding function
        """
        return Field(json_schema_extra={"source_column_for": self}, **kwargs)

    def VectorField(self, **kwargs):
        """
        Creates a pydantic Field that can automatically annotate
        the target vector column for this embedding function
        """
        return Field(json_schema_extra={"vector_column_for": self}, **kwargs)


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

    def compute_query_embeddings(self, query: str, *args, **kwargs) -> List[np.array]:
        return self.compute_source_embeddings(query, *args, **kwargs)

    def compute_source_embeddings(self, texts: TEXT, *args, **kwargs) -> List[np.array]:
        texts = self.sanitize_input(texts)
        return self.generate_embeddings(texts)

    @abstractmethod
    def generate_embeddings(
        self, texts: Union[List[str], np.ndarray]
    ) -> List[np.array]:
        """
        Generate the embeddings for the given texts
        """
        pass


# @EmbeddingFunctionRegistry.get_instance().register(name) doesn't work in 3.8
register = lambda name: EmbeddingFunctionRegistry.get_instance().register(name)


@register("sentence-transformers")
class SentenceTransformerEmbeddings(TextEmbeddingFunction):
    """
    An embedding function that uses the sentence-transformers library

    https://huggingface.co/sentence-transformers
    """

    name: str = "all-MiniLM-L6-v2"
    device: str = "cpu"
    normalize: bool = True

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._ndims = None

    @property
    def embedding_model(self):
        """
        Get the sentence-transformers embedding model specified by the
        name and device. This is cached so that the model is only loaded
        once per process.
        """
        return self.__class__.get_embedding_model(self.name, self.device)

    def ndims(self):
        if self._ndims is None:
            self._ndims = len(self.generate_embeddings("foo")[0])
        return self._ndims

    def generate_embeddings(
        self, texts: Union[List[str], np.ndarray]
    ) -> List[np.array]:
        """
        Get the embeddings for the given texts

        Parameters
        ----------
        texts: list[str] or np.ndarray (of str)
            The texts to embed
        """
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

        TODO: use lru_cache instead with a reasonable/configurable maxsize
        """
        sentence_transformers = cls.safe_import(
            "sentence_transformers", "sentence-transformers"
        )
        return sentence_transformers.SentenceTransformer(name, device=device)


@register("openai")
class OpenAIEmbeddings(TextEmbeddingFunction):
    """
    An embedding function that uses the OpenAI API

    https://platform.openai.com/docs/guides/embeddings
    """

    name: str = "text-embedding-ada-002"

    def ndims(self):
        # TODO don't hardcode this
        return 1536

    def generate_embeddings(
        self, texts: Union[List[str], np.ndarray]
    ) -> List[np.array]:
        """
        Get the embeddings for the given texts

        Parameters
        ----------
        texts: list[str] or np.ndarray (of str)
            The texts to embed
        """
        # TODO retry, rate limit, token limit
        openai = self.safe_import("openai")
        rs = openai.Embedding.create(input=texts, model=self.name)["data"]
        return [v["embedding"] for v in rs]


@register("open-clip")
class OpenClipEmbeddings(EmbeddingFunction):
    """
    An embedding function that uses the OpenClip API
    For multi-modal text-to-image search

    https://github.com/mlfoundations/open_clip
    """

    name: str = "ViT-B-32"
    pretrained: str = "laion2b_s34b_b79k"
    device: str = "cpu"
    batch_size: int = 64
    normalize: bool = True
    _model = PrivateAttr()
    _preprocess = PrivateAttr()
    _tokenizer = PrivateAttr()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        open_clip = self.safe_import("open_clip", "open-clip")
        model, _, preprocess = open_clip.create_model_and_transforms(
            self.name, pretrained=self.pretrained
        )
        model.to(self.device)
        self._model, self._preprocess = model, preprocess
        self._tokenizer = open_clip.get_tokenizer(self.name)
        self._ndims = None

    def ndims(self):
        if self._ndims is None:
            self._ndims = self.generate_text_embeddings("foo").shape[0]
        return self._ndims

    def compute_query_embeddings(
        self, query: Union[str, "PIL.Image.Image"], *args, **kwargs
    ) -> List[np.ndarray]:
        """
        Compute the embeddings for a given user query

        Parameters
        ----------
        query : Union[str, PIL.Image.Image]
            The query to embed. A query can be either text or an image.
        """
        if isinstance(query, str):
            return [self.generate_text_embeddings(query)]
        else:
            PIL = self.safe_import("PIL", "pillow")
            if isinstance(query, PIL.Image.Image):
                return [self.generate_image_embedding(query)]
            else:
                raise TypeError("OpenClip supports str or PIL Image as query")

    def generate_text_embeddings(self, text: str) -> np.ndarray:
        torch = self.safe_import("torch")
        text = self.sanitize_input(text)
        text = self._tokenizer(text)
        text.to(self.device)
        with torch.no_grad():
            text_features = self._model.encode_text(text.to(self.device))
            if self.normalize:
                text_features /= text_features.norm(dim=-1, keepdim=True)
            return text_features.cpu().numpy().squeeze()

    def sanitize_input(self, images: IMAGES) -> Union[List[bytes], np.ndarray]:
        """
        Sanitize the input to the embedding function.
        """
        if isinstance(images, (str, bytes)):
            images = [images]
        elif isinstance(images, pa.Array):
            images = images.to_pylist()
        elif isinstance(images, pa.ChunkedArray):
            images = images.combine_chunks().to_pylist()
        return images

    def compute_source_embeddings(
        self, images: IMAGES, *args, **kwargs
    ) -> List[np.array]:
        """
        Get the embeddings for the given images
        """
        images = self.sanitize_input(images)
        embeddings = []
        for i in range(0, len(images), self.batch_size):
            j = min(i + self.batch_size, len(images))
            batch = images[i:j]
            embeddings.extend(self._parallel_get(batch))
        return embeddings

    def _parallel_get(self, images: Union[List[str], List[bytes]]) -> List[np.ndarray]:
        """
        Issue concurrent requests to retrieve the image data
        """
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(self.generate_image_embedding, image)
                for image in images
            ]
            return [future.result() for future in tqdm(futures)]

    def generate_image_embedding(
        self, image: Union[str, bytes, "PIL.Image.Image"]
    ) -> np.ndarray:
        """
        Generate the embedding for a single image

        Parameters
        ----------
        image : Union[str, bytes, PIL.Image.Image]
            The image to embed. If the image is a str, it is treated as a uri.
            If the image is bytes, it is treated as the raw image bytes.
        """
        torch = self.safe_import("torch")
        # TODO handle retry and errors for https
        image = self._to_pil(image)
        image = self._preprocess(image).unsqueeze(0)
        with torch.no_grad():
            return self._encode_and_normalize_image(image)

    def _to_pil(self, image: Union[str, bytes]):
        PIL = self.safe_import("PIL", "pillow")
        if isinstance(image, bytes):
            return PIL.Image.open(io.BytesIO(image))
        if isinstance(image, PIL.Image.Image):
            return image
        elif isinstance(image, str):
            parsed = urlparse.urlparse(image)
            # TODO handle drive letter on windows.
            if parsed.scheme == "file":
                return PIL.Image.open(parsed.path)
            elif parsed.scheme == "":
                return PIL.Image.open(image if os.name == "nt" else parsed.path)
            elif parsed.scheme.startswith("http"):
                return PIL.Image.open(io.BytesIO(url_retrieve(image)))
            else:
                raise NotImplementedError("Only local and http(s) urls are supported")

    def _encode_and_normalize_image(self, image_tensor: "torch.Tensor"):
        """
        encode a single image tensor and optionally normalize the output
        """
        image_features = self._model.encode_image(image_tensor.to(self.device))
        if self.normalize:
            image_features /= image_features.norm(dim=-1, keepdim=True)
        return image_features.cpu().numpy().squeeze()


def url_retrieve(url: str):
    """
    Parameters
    ----------
    url: str
        URL to download from
    """
    try:
        with urllib.request.urlopen(url) as conn:
            return conn.read()
    except (socket.gaierror, urllib.error.URLError) as err:
        raise ConnectionError("could not download {} due to {}".format(url, err))
