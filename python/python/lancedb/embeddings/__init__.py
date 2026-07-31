# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors


# ruff: noqa: F401
from .base import EmbeddingFunction, EmbeddingFunctionConfig, TextEmbeddingFunction
from .bedrock import BedRockText
from .cohere import CohereEmbeddingFunction
from .gemini_text import GeminiText
from .instructor import InstructorEmbeddingFunction
from .ollama import OllamaEmbeddings
from .open_clip import OpenClipEmbeddings
from .openai import OpenAIEmbeddings
from .registry import EmbeddingFunctionRegistry, get_registry, register
from .sentence_transformers import SentenceTransformerEmbeddings
from .gte import GteEmbeddings
from .transformers import TransformersEmbeddingFunction, ColbertEmbeddings
from .imagebind import ImageBindEmbeddings
from .jinaai import JinaEmbeddings
from .watsonx import WatsonxEmbeddings
from .voyageai import VoyageAIEmbeddingFunction
from .colpali import ColPaliEmbeddings
from .siglip import SigLipEmbeddings

# The API reference renders this package with a single mkdocstrings directive,
# which only picks up names listed here. New embedding functions must be added
# to both the imports above and this list, or they will silently go undocumented.
__all__ = [
    "EmbeddingFunction",
    "EmbeddingFunctionConfig",
    "TextEmbeddingFunction",
    "EmbeddingFunctionRegistry",
    "get_registry",
    "register",
    "SentenceTransformerEmbeddings",
    "OpenAIEmbeddings",
    "OpenClipEmbeddings",
    "BedRockText",
    "CohereEmbeddingFunction",
    "GeminiText",
    "GteEmbeddings",
    "InstructorEmbeddingFunction",
    "JinaEmbeddings",
    "OllamaEmbeddings",
    "TransformersEmbeddingFunction",
    "ColbertEmbeddings",
    "VoyageAIEmbeddingFunction",
    "WatsonxEmbeddings",
    "ColPaliEmbeddings",
    "ImageBindEmbeddings",
    "SigLipEmbeddings",
]
