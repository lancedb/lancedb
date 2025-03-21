# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

from typing import Literal

# Query type literals
QueryType = Literal["vector", "fts", "hybrid", "auto"]

# Distance type literals
DistanceType = Literal["l2", "cosine", "dot"]
DistanceTypeWithHamming = Literal["l2", "cosine", "dot", "hamming"]

# Vector handling literals
OnBadVectorsType = Literal["error", "drop", "fill", "null"]

# Mode literals
AddMode = Literal["append", "overwrite"]
CreateMode = Literal["create", "overwrite"]

# Index type literals
VectorIndexType = Literal["IVF_FLAT", "IVF_PQ", "IVF_HNSW_SQ", "IVF_HNSW_PQ"]
ScalarIndexType = Literal["BTREE", "BITMAP", "LABEL_LIST"]
IndexType = Literal[
    "IVF_PQ", "IVF_HNSW_PQ", "IVF_HNSW_SQ", "FTS", "BTREE", "BITMAP", "LABEL_LIST"
]

# Tokenizer literals
BaseTokenizerType = Literal["simple", "raw", "whitespace"]
