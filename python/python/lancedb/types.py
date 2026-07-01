# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

from __future__ import annotations

from typing import Dict, List, Literal, Optional, Tuple, Union

from .expr import Expr

# Query type literals
QueryType = Literal["vector", "fts", "hybrid", "auto"]

BlobMode = Literal["lazy", "bytes", "descriptions"]

QueryProjectionSpec = Union[
    List[str],
    List[Tuple[str, Union[str, Expr]]],
    Dict[str, Union[str, Expr]],
]
QueryProjection = Optional[QueryProjectionSpec]

# Distance type literals
DistanceType = Literal["l2", "cosine", "dot"]
DistanceTypeWithHamming = Literal["l2", "cosine", "dot", "hamming"]

# Vector handling literals
OnBadVectorsType = Literal["error", "drop", "fill", "null"]

# Mode literals
AddMode = Literal["append", "overwrite"]
CreateMode = Literal["create", "overwrite"]

# Index type literals
VectorIndexType = Literal[
    "IVF_FLAT",
    "IVF_SQ",
    "IVF_PQ",
    "IVF_HNSW_SQ",
    "IVF_HNSW_PQ",
    "IVF_HNSW_FLAT",
    "IVF_RQ",
]
ScalarIndexType = Literal["BTREE", "BITMAP", "LABEL_LIST"]
IndexType = Literal[
    "IVF_PQ",
    "IVF_HNSW_PQ",
    "IVF_HNSW_SQ",
    "IVF_HNSW_FLAT",
    "IVF_SQ",
    "FTS",
    "BTREE",
    "BITMAP",
    "LABEL_LIST",
    "IVF_RQ",
]

# Tokenizer literals
BuiltinTokenizerType = Literal["simple", "raw", "whitespace", "ngram"]
BaseTokenizerType = BuiltinTokenizerType | str
