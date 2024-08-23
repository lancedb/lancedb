from .answerdotai import AnswerdotaiRerankers


class ColbertReranker(AnswerdotaiRerankers):
    """
    Reranks the results using the ColBERT model.

    Parameters
    ----------
    model_name : str, default "colbert" (colbert-ir/colbert-v2.0)
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
        super().__init__(
            model_type="colbert",
            model_name=model_name,
            column=column,
            return_score=return_score,
        )
