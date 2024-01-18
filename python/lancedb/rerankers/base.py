import importlib
from typing import Any
import lancedb
from abc import ABC, abstractmethod
import pyarrow as pa
from ..utils.general import LOGGER

class Reranker(ABC):

    @abstractmethod
    def rerank_hybrid(query_builder: "lancedb.HybridQueryBuilder", vector_results: pa.Table, fts_results: pa.Table, combined_results: pa.Table):
        """
        Rerank function recieves the individual results from the vector and FTS search and the combined results from the default combination logic.
        You can choose to use any of the results to generate the final results, allowing maximum flexibility. This is mandatory to implement
        
        Parameters
        ----------
        query_builder : "lancedb.HybridQueryBuilder"
            The query builder object that was used to generate the results
        vector_results : pa.Table
            The results from the vector search
        fts_results : pa.Table
            The results from the FTS search
        combined_results : pa.Table
            The combined results from the vector and FTS search using the default combination logic
        """
        pass

    def rerank_vector(query_builder: "lancedb.VectorQueryBuilder", vector_results: pa.Table):
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

    @staticmethod
    def api_key_not_found_help(provider):
        LOGGER.error(f"Could not find API key for {provider}.")
        raise ValueError(f"Please set the {provider.upper()}_API_KEY environment variable.")