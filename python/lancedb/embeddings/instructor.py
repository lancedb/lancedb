import os
from typing import ClassVar, List, Union
from cachetools import cached
from functools import lru_cache

import numpy as np

from .base import TextEmbeddingFunction
from .registry import register
from .utils import TEXT


@register("instructor")
class InstuctorEmbeddingFunction(TextEmbeddingFunction):
    """
    If you want to calculate customized embeddings for specific sentences, you may follow the unified template to write instructions:

        "Represent the `domain` `text_type` for `task_objective`":

        * domain is optional, and it specifies the domain of the text, e.g., science, finance, medicine, etc.
        * text_type is required, and it specifies the encoding unit, e.g., sentence, document, paragraph, etc.
        * task_objective is optional, and it specifies the objective of embedding, e.g., retrieve a document, classify the sentence, etc.
    """
    name: str = "hkunlp/instructor-base"
    batch_size: int = 32
    device: str = "cpu"
    show_progress_bar: bool = True
    normalize_embeddings: bool = True
    # convert_to_numpy: bool = True # Hardcoding this as numpy can be ingested directly

    source_instruction: str = "represent the docuement for retreival"
    query_instruction: str = "represent the document for retreiving the most similar documents"


    #@lru_cache(maxsize=1)
    def ndims(self):
        model = self.get_model()
        return model.encode("foo").shape[0]

    def compute_query_embeddings(self, query: str, *args, **kwargs) -> List[np.array]:
        query = self.sanitize_input(query)
        return self.generate_embeddings([[self.query_instruction, query]])

    def compute_source_embeddings(self, texts: TEXT, *args, **kwargs) -> List[np.array]:
        texts = self.sanitize_input(texts)
        texts_formatted = []
        for text in texts:
            texts_formatted.append([self.source_instruction, text])
        return self.generate_embeddings(texts_formatted)
    
    def generate_embeddings(self, texts: List) -> List:
        model = self.get_model()
        return model.encode(texts, batch_size=self.batch_size, show_progress_bar=self.show_progress_bar, normalize_embeddings=self.normalize_embeddings).tolist()
    
    #@cached(cache={})
    def get_model(self):
        instructor_embedding = self.safe_import("InstructorEmbedding", "InstructorEmbedding")

        model = instructor_embedding.INSTRUCTOR(self.name)
        return model
