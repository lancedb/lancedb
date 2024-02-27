from functools import cached_property

import pyarrow as pa

from ..util import attempt_import_or_raise
from .base import Reranker


class ColbertReranker(Reranker):
    """
    Reranks the results using the ColBERT model.

    Parameters
    ----------
    model_name : str, default "colbert-ir/colbertv2.0"
        The name of the cross encoder model to use.
    column : str, default "text"
        The name of the column to use as input to the cross encoder model.
    return_score : str, default "relevance"
        options are "relevance" or "all". Only "relevance" is supported for now.
    """

    def __init__(
        self,
        model_name: str = "colbert-ir/colbertv2.0",
        column: str = "text",
        return_score="relevance",
    ):
        super().__init__(return_score)
        self.model_name = model_name
        self.column = column
        self.torch = attempt_import_or_raise(
            "torch"
        )  # import here for faster ops later

    def rerank_hybrid(
        self,
        query: str,
        vector_results: pa.Table,
        fts_results: pa.Table,
    ):
        combined_results = self.merge_results(vector_results, fts_results)
        docs = combined_results[self.column].to_pylist()

        tokenizer, model = self._model

        # Encode the query
        query_encoding = tokenizer(query, return_tensors="pt")
        query_embedding = model(**query_encoding).last_hidden_state.mean(dim=1)
        scores = []
        # Get score for each document
        for document in docs:
            document_encoding = tokenizer(
                document, return_tensors="pt", truncation=True, max_length=512
            )
            document_embedding = model(**document_encoding).last_hidden_state
            # Calculate MaxSim score
            score = self.maxsim(query_embedding.unsqueeze(0), document_embedding)
            scores.append(score.item())

        # replace the self.column column with the docs
        combined_results = combined_results.drop(self.column)
        combined_results = combined_results.append_column(
            self.column, pa.array(docs, type=pa.string())
        )
        # add the scores
        combined_results = combined_results.append_column(
            "_relevance_score", pa.array(scores, type=pa.float32())
        )
        if self.score == "relevance":
            combined_results = combined_results.drop_columns(["score", "_distance"])
        elif self.score == "all":
            raise NotImplementedError(
                "OpenAI Reranker does not support score='all' yet"
            )

        combined_results = combined_results.sort_by(
            [("_relevance_score", "descending")]
        )

        return combined_results

    @cached_property
    def _model(self):
        transformers = attempt_import_or_raise("transformers")
        tokenizer = transformers.AutoTokenizer.from_pretrained(self.model_name)
        model = transformers.AutoModel.from_pretrained(self.model_name)

        return tokenizer, model

    def maxsim(self, query_embedding, document_embedding):
        # Expand dimensions for broadcasting
        # Query: [batch, length, size] -> [batch, query, 1, size]
        # Document: [batch, length, size] -> [batch, 1, length, size]
        expanded_query = query_embedding.unsqueeze(2)
        expanded_doc = document_embedding.unsqueeze(1)

        # Compute cosine similarity across the embedding dimension
        sim_matrix = self.torch.nn.functional.cosine_similarity(
            expanded_query, expanded_doc, dim=-1
        )

        # Take the maximum similarity for each query token (across all document tokens)
        # sim_matrix shape: [batch_size, query_length, doc_length]
        max_sim_scores, _ = self.torch.max(sim_matrix, dim=2)

        # Average these maximum scores across all query tokens
        avg_max_sim = self.torch.mean(max_sim_scores, dim=1)
        return avg_max_sim
