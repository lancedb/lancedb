import importlib
from typing import Any
import lancedb
from abc import ABC, abstractmethod
import pyarrow as pa
from ..utils.general import LOGGER


class Reranker(ABC):
    @abstractmethod
    def rerank_hybrid(
        query_builder: "lancedb.HybridQueryBuilder",
        vector_results: pa.Table,
        fts_results: pa.Table,
    ):
        """
        Rerank function receives the individual results from the vector and FTS search results.
        You can choose to use any of the results to generate the final results, allowing maximum flexibility. This is mandatory to implement

        Parameters
        ----------
        query_builder : "lancedb.HybridQueryBuilder"
            The query builder object that was used to generate the results
        vector_results : pa.Table
            The results from the vector search
        fts_results : pa.Table
            The results from the FTS search
        """
        pass

    def rerank_vector(
        query_builder: "lancedb.VectorQueryBuilder", vector_results: pa.Table
    ):
        """
        Rerank function recieves the individual results from the vector search. This isn't mandatory to implement

        Parameters
        ----------
        query_builder : "lancedb.VectorQueryBuilder"
            The query builder object that was used to generate the results
        vector_results : pa.Table
            The results from the vector search
        """
        raise NotImplementedError("Vector Reranking is not implemented")

    def rerank_fts(query_builder: "lancedb.FTSQueryBuilder", fts_results: pa.Table):
        """
        Rerank function recieves the individual results from the FTS search. This isn't mandatory to implement

        Parameters
        ----------
        query_builder : "lancedb.FTSQueryBuilder"
            The query builder object that was used to generate the results
        fts_results : pa.Table
            The results from the FTS search
        """
        raise NotImplementedError("FTS Reranking is not implemented")

    def merge_results(self, vector_results: pa.Table, fts_results: pa.Table):
        """
        Merge the results from the vector and FTS search. This is a vanilla merging function that just concatenates the results and removes
        the duplicates.

        Parameters
        ----------
        vector_results : pa.Table
            The results from the vector search
        fts_results : pa.Table
            The results from the FTS search
        """
        ## !!!! TODO: This op is inefficient. couldn't make pa.concat_tables to work. Also need to look into pa.compute.unique
        vector_list = vector_results.to_pylist()
        fts_list = fts_results.to_pylist()
        combined_df = vector_list + fts_list

        unique_row_ids = set()
        unique_rows = []
        for row in combined_df:
            row_id = row["_rowid"]
            if row_id not in unique_row_ids:
                unique_row_ids.add(row_id)
                unique_rows.append(row)

        combined_results = pa.Table.from_pylist(
            unique_rows, schema=vector_results.schema
        )
        return combined_results
