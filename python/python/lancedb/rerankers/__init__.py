# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

from .base import Reranker
from .cohere import CohereReranker
from .colbert import ColbertReranker
from .cross_encoder import CrossEncoderReranker
from .linear_combination import LinearCombinationReranker
from .openai import OpenaiReranker
from .jinaai import JinaReranker
from .rrf import RRFReranker
from .mrr import MRRReranker
from .answerdotai import AnswerdotaiRerankers
from .voyageai import VoyageAIReranker
from .watsonx import WatsonxReranker

# The API reference renders this module with a single mkdocstrings directive,
# which only picks up names listed here. New public names must be added to this
# list, or they will silently go undocumented.
__all__ = [
    "Reranker",
    "CrossEncoderReranker",
    "CohereReranker",
    "LinearCombinationReranker",
    "OpenaiReranker",
    "ColbertReranker",
    "JinaReranker",
    "RRFReranker",
    "AnswerdotaiRerankers",
    "VoyageAIReranker",
    "MRRReranker",
    "WatsonxReranker",
]
