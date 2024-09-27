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
    **kwargs
        Additional keyword arguments to pass to the model, for example, 'device'.
        See AnswerDotAI/rerankers for more information.
    """

    def __init__(
        self,
        model_name: str = "colbert-ir/colbertv2.0",
        column: str = "text",
        return_score="relevance",
        **kwargs,
    ):
        super().__init__(
            model_type="colbert",
            model_name=model_name,
            column=column,
            return_score=return_score,
            **kwargs,
        )
