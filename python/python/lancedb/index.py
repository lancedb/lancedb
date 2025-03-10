# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

from dataclasses import dataclass
from typing import Literal, Optional

from ._lancedb import (
    IndexConfig,
)

lang_mapping = {
    "ar": "Arabic",
    "da": "Danish",
    "du": "Dutch",
    "en": "English",
    "fi": "Finnish",
    "fr": "French",
    "de": "German",
    "gr": "Greek",
    "hu": "Hungarian",
    "it": "Italian",
    "no": "Norwegian",
    "pt": "Portuguese",
    "ro": "Romanian",
    "ru": "Russian",
    "es": "Spanish",
    "sv": "Swedish",
    "ta": "Tamil",
    "tr": "Turkish",
}


@dataclass
class BTree:
    """Describes a btree index configuration

    A btree index is an index on scalar columns.  The index stores a copy of the
    column in sorted order.  A header entry is created for each block of rows
    (currently the block size is fixed at 4096).  These header entries are stored
    in a separate cacheable structure (a btree).  To search for data the header is
    used to determine which blocks need to be read from disk.

    For example, a btree index in a table with 1Bi rows requires
    sizeof(Scalar) * 256Ki bytes of memory and will generally need to read
    sizeof(Scalar) * 4096 bytes to find the correct row ids.

    This index is good for scalar columns with mostly distinct values and does best
    when the query is highly selective. It works with numeric, temporal, and string
    columns.

    The btree index does not currently have any parameters though parameters such as
    the block size may be added in the future.
    """

    pass


@dataclass
class Bitmap:
    """Describe a Bitmap index configuration.

    A `Bitmap` index stores a bitmap for each distinct value in the column for
    every row.

    This index works best for low-cardinality numeric or string columns,
    where the number of unique values is small (i.e., less than a few thousands).
    `Bitmap` index can accelerate the following filters:

    - `<`, `<=`, `=`, `>`, `>=`
    - `IN (value1, value2, ...)`
    - `between (value1, value2)`
    - `is null`

    For example, a bitmap index with a table with 1Bi rows, and 128 distinct values,
    requires 128 / 8 * 1Bi bytes on disk.
    """

    pass


@dataclass
class LabelList:
    """Describe a LabelList index configuration.

    `LabelList` is a scalar index that can be used on `List<T>` columns to
    support queries with `array_contains_all` and `array_contains_any`
    using an underlying bitmap index.

    For example, it works with `tags`, `categories`, `keywords`, etc.
    """

    pass


@dataclass
class FTS:
    """Describe a FTS index configuration.

    `FTS` is a full-text search index that can be used on `String` columns

    For example, it works with `title`, `description`, `content`, etc.

    Attributes
    ----------
    with_position : bool, default True
        Whether to store the position of the token in the document. Setting this
        to False can reduce the size of the index and improve indexing speed,
        but it will disable support for phrase queries.
    base_tokenizer : str, default "simple"
        The base tokenizer to use for tokenization. Options are:
        - "simple": Splits text by whitespace and punctuation.
        - "whitespace": Split text by whitespace, but not punctuation.
        - "raw": No tokenization. The entire text is treated as a single token.
    language : str, default "English"
        The language to use for tokenization.
    max_token_length : int, default 40
        The maximum token length to index. Tokens longer than this length will be
        ignored.
    lower_case : bool, default True
        Whether to convert the token to lower case. This makes queries case-insensitive.
    stem : bool, default False
        Whether to stem the token. Stemming reduces words to their root form.
        For example, in English "running" and "runs" would both be reduced to "run".
    remove_stop_words : bool, default False
        Whether to remove stop words. Stop words are common words that are often
        removed from text before indexing. For example, in English "the" and "and".
    ascii_folding : bool, default False
        Whether to fold ASCII characters. This converts accented characters to
        their ASCII equivalent. For example, "café" would be converted to "cafe".
    """

    with_position: bool = True
    base_tokenizer: Literal["simple", "raw", "whitespace"] = "simple"
    language: str = "English"
    max_token_length: Optional[int] = 40
    lower_case: bool = True
    stem: bool = False
    remove_stop_words: bool = False
    ascii_folding: bool = False


@dataclass
class HnswPq:
    """Describe a HNSW-PQ index configuration.

    HNSW-PQ stands for Hierarchical Navigable Small World - Product Quantization.
    It is a variant of the HNSW algorithm that uses product quantization to compress
    the vectors. To create an HNSW-PQ index, you can specify the following parameters:

    Parameters
    ----------

    distance_type: str, default "l2"

        The distance metric used to train the index.

        The following distance types are available:

        "l2" - Euclidean distance. This is a very common distance metric that
        accounts for both magnitude and direction when determining the distance
        between vectors. l2 distance has a range of [0, ∞).

        "cosine" - Cosine distance.  Cosine distance is a distance metric
        calculated from the cosine similarity between two vectors. Cosine
        similarity is a measure of similarity between two non-zero vectors of an
        inner product space. It is defined to equal the cosine of the angle
        between them.  Unlike l2, the cosine distance is not affected by the
        magnitude of the vectors.  Cosine distance has a range of [0, 2].

        "dot" - Dot product. Dot distance is the dot product of two vectors. Dot
        distance has a range of (-∞, ∞). If the vectors are normalized (i.e. their
        l2 norm is 1), then dot distance is equivalent to the cosine distance.

    num_partitions, default sqrt(num_rows)

        The number of IVF partitions to create.

        For HNSW, we recommend a small number of partitions. Setting this to 1 works
        well for most tables. For very large tables, training just one HNSW graph
        will require too much memory. Each partition becomes its own HNSW graph, so
        setting this value higher reduces the peak memory use of training.

    num_sub_vectors, default is vector dimension / 16

        Number of sub-vectors of PQ.

        This value controls how much the vector is compressed during the
        quantization step. The more sub vectors there are the less the vector is
        compressed.  The default is the dimension of the vector divided by 16.
        If the dimension is not evenly divisible by 16 we use the dimension
        divided by 8.

        The above two cases are highly preferred.  Having 8 or 16 values per
        subvector allows us to use efficient SIMD instructions.

        If the dimension is not visible by 8 then we use 1 subvector.  This is not
        ideal and will likely result in poor performance.

     num_bits: int, default 8
        Number of bits to encode each sub-vector.

        This value controls how much the sub-vectors are compressed.  The more bits
        the more accurate the index but the slower search. Only 4 and 8 are supported.

    max_iterations, default 50

        Max iterations to train kmeans.

        When training an IVF index we use kmeans to calculate the partitions.  This
        parameter controls how many iterations of kmeans to run.

        Increasing this might improve the quality of the index but in most cases the
        parameter is unused because kmeans will converge with fewer iterations.  The
        parameter is only used in cases where kmeans does not appear to converge.  In
        those cases it is unlikely that setting this larger will lead to the index
        converging anyways.

    sample_rate, default 256

        The rate used to calculate the number of training vectors for kmeans.

        When an IVF index is trained, we need to calculate partitions.  These are
        groups of vectors that are similar to each other.  To do this we use an
        algorithm called kmeans.

        Running kmeans on a large dataset can be slow.  To speed this up we
        run kmeans on a random sample of the data.  This parameter controls the
        size of the sample.  The total number of vectors used to train the index
        is `sample_rate * num_partitions`.

        Increasing this value might improve the quality of the index but in
        most cases the default should be sufficient.

    m, default 20

        The number of neighbors to select for each vector in the HNSW graph.

        This value controls the tradeoff between search speed and accuracy.
        The higher the value the more accurate the search but the slower it will be.

    ef_construction, default 300

        The number of candidates to evaluate during the construction of the HNSW graph.

        This value controls the tradeoff between build speed and accuracy.
        The higher the value the more accurate the build but the slower it will be.
        150 to 300 is the typical range. 100 is a minimum for good quality search
        results. In most cases, there is no benefit to setting this higher than 500.
        This value should be set to a value that is not less than `ef` in the
        search phase.
    """

    distance_type: Literal["l2", "cosine", "dot"] = "l2"
    num_partitions: Optional[int] = None
    num_sub_vectors: Optional[int] = None
    num_bits: int = 8
    max_iterations: int = 50
    sample_rate: int = 256
    m: int = 20
    ef_construction: int = 300


@dataclass
class HnswSq:
    """Describe a HNSW-SQ index configuration.

    HNSW-SQ stands for Hierarchical Navigable Small World - Scalar Quantization.
    It is a variant of the HNSW algorithm that uses scalar quantization to compress
    the vectors.

    Parameters
    ----------

    distance_type: str, default "l2"

        The distance metric used to train the index.

        The following distance types are available:

        "l2" - Euclidean distance. This is a very common distance metric that
        accounts for both magnitude and direction when determining the distance
        between vectors. l2 distance has a range of [0, ∞).

        "cosine" - Cosine distance.  Cosine distance is a distance metric
        calculated from the cosine similarity between two vectors. Cosine
        similarity is a measure of similarity between two non-zero vectors of an
        inner product space. It is defined to equal the cosine of the angle
        between them.  Unlike l2, the cosine distance is not affected by the
        magnitude of the vectors.  Cosine distance has a range of [0, 2].

        "dot" - Dot product. Dot distance is the dot product of two vectors. Dot
        distance has a range of (-∞, ∞). If the vectors are normalized (i.e. their
        l2 norm is 1), then dot distance is equivalent to the cosine distance.

    num_partitions, default sqrt(num_rows)

        The number of IVF partitions to create.

        For HNSW, we recommend a small number of partitions. Setting this to 1 works
        well for most tables. For very large tables, training just one HNSW graph
        will require too much memory. Each partition becomes its own HNSW graph, so
        setting this value higher reduces the peak memory use of training.

    max_iterations, default 50

        Max iterations to train kmeans.

        When training an IVF index we use kmeans to calculate the partitions.
        This parameter controls how many iterations of kmeans to run.

        Increasing this might improve the quality of the index but in most cases
        the parameter is unused because kmeans will converge with fewer iterations.
        The parameter is only used in cases where kmeans does not appear to converge.
        In those cases it is unlikely that setting this larger will lead to
        the index converging anyways.

    sample_rate, default 256

        The rate used to calculate the number of training vectors for kmeans.

        When an IVF index is trained, we need to calculate partitions.  These
        are groups of vectors that are similar to each other.  To do this
        we use an algorithm called kmeans.

        Running kmeans on a large dataset can be slow.  To speed this up we
        run kmeans on a random sample of the data.  This parameter controls the
        size of the sample.  The total number of vectors used to train the index
        is `sample_rate * num_partitions`.

        Increasing this value might improve the quality of the index but in
        most cases the default should be sufficient.

    m, default 20

        The number of neighbors to select for each vector in the HNSW graph.

        This value controls the tradeoff between search speed and accuracy.
        The higher the value the more accurate the search but the slower it will be.

    ef_construction, default 300

        The number of candidates to evaluate during the construction of the HNSW graph.

        This value controls the tradeoff between build speed and accuracy.
        The higher the value the more accurate the build but the slower it will be.
        150 to 300 is the typical range. 100 is a minimum for good quality search
        results. In most cases, there is no benefit to setting this higher than 500.
        This value should be set to a value that is not less than `ef` in the search
        phase.

    """

    distance_type: Literal["l2", "cosine", "dot"] = "l2"
    num_partitions: Optional[int] = None
    max_iterations: int = 50
    sample_rate: int = 256
    m: int = 20
    ef_construction: int = 300


@dataclass
class IvfFlat:
    """Describes an IVF Flat Index

    This index stores raw vectors.
    These vectors are grouped into partitions of similar vectors.
    Each partition keeps track of a centroid which is
    the average value of all vectors in the group.

    Attributes
    ----------
    distance_type: str, default "l2"
        The distance metric used to train the index

        This is used when training the index to calculate the IVF partitions
        (vectors are grouped in partitions with similar vectors according to this
        distance type) and to calculate a subvector's code during quantization.

        The distance type used to train an index MUST match the distance type used
        to search the index.  Failure to do so will yield inaccurate results.

        The following distance types are available:

        "l2" - Euclidean distance. This is a very common distance metric that
        accounts for both magnitude and direction when determining the distance
        between vectors. l2 distance has a range of [0, ∞).

        "cosine" - Cosine distance.  Cosine distance is a distance metric
        calculated from the cosine similarity between two vectors. Cosine
        similarity is a measure of similarity between two non-zero vectors of an
        inner product space. It is defined to equal the cosine of the angle
        between them.  Unlike l2, the cosine distance is not affected by the
        magnitude of the vectors.  Cosine distance has a range of [0, 2].

        Note: the cosine distance is undefined when one (or both) of the vectors
        are all zeros (there is no direction).  These vectors are invalid and may
        never be returned from a vector search.

        "dot" - Dot product. Dot distance is the dot product of two vectors. Dot
        distance has a range of (-∞, ∞). If the vectors are normalized (i.e. their
        l2 norm is 1), then dot distance is equivalent to the cosine distance.

        "hamming" - Hamming distance. Hamming distance is a distance metric
        calculated as the number of positions at which the corresponding bits are
        different. Hamming distance has a range of [0, vector dimension].

    num_partitions: int, default sqrt(num_rows)
        The number of IVF partitions to create.

        This value should generally scale with the number of rows in the dataset.
        By default the number of partitions is the square root of the number of
        rows.

        If this value is too large then the first part of the search (picking the
        right partition) will be slow.  If this value is too small then the second
        part of the search (searching within a partition) will be slow.

    max_iterations: int, default 50
        Max iteration to train kmeans.

        When training an IVF PQ index we use kmeans to calculate the partitions.
        This parameter controls how many iterations of kmeans to run.

        Increasing this might improve the quality of the index but in most cases
        these extra iterations have diminishing returns.

        The default value is 50.
    sample_rate: int, default 256
        The rate used to calculate the number of training vectors for kmeans.

        When an IVF PQ index is trained, we need to calculate partitions.  These
        are groups of vectors that are similar to each other.  To do this we use an
        algorithm called kmeans.

        Running kmeans on a large dataset can be slow.  To speed this up we run
        kmeans on a random sample of the data.  This parameter controls the size of
        the sample.  The total number of vectors used to train the index is
        `sample_rate * num_partitions`.

        Increasing this value might improve the quality of the index but in most
        cases the default should be sufficient.

        The default value is 256.
    """

    distance_type: Literal["l2", "cosine", "dot", "hamming"] = "l2"
    num_partitions: Optional[int] = None
    max_iterations: int = 50
    sample_rate: int = 256


@dataclass
class IvfPq:
    """Describes an IVF PQ Index

    This index stores a compressed (quantized) copy of every vector.  These vectors
    are grouped into partitions of similar vectors.  Each partition keeps track of
    a centroid which is the average value of all vectors in the group.

    During a query the centroids are compared with the query vector to find the
    closest partitions.  The compressed vectors in these partitions are then
    searched to find the closest vectors.

    The compression scheme is called product quantization.  Each vector is divide
    into subvectors and then each subvector is quantized into a small number of
    bits.  the parameters `num_bits` and `num_subvectors` control this process,
    providing a tradeoff between index size (and thus search speed) and index
    accuracy.

    The partitioning process is called IVF and the `num_partitions` parameter
    controls how many groups to create.

    Note that training an IVF PQ index on a large dataset is a slow operation and
    currently is also a memory intensive operation.

    Attributes
    ----------
    distance_type: str, default "l2"
        The distance metric used to train the index

        This is used when training the index to calculate the IVF partitions
        (vectors are grouped in partitions with similar vectors according to this
        distance type) and to calculate a subvector's code during quantization.

        The distance type used to train an index MUST match the distance type used
        to search the index.  Failure to do so will yield inaccurate results.

        The following distance types are available:

        "l2" - Euclidean distance. This is a very common distance metric that
        accounts for both magnitude and direction when determining the distance
        between vectors. l2 distance has a range of [0, ∞).

        "cosine" - Cosine distance.  Cosine distance is a distance metric
        calculated from the cosine similarity between two vectors. Cosine
        similarity is a measure of similarity between two non-zero vectors of an
        inner product space. It is defined to equal the cosine of the angle
        between them.  Unlike l2, the cosine distance is not affected by the
        magnitude of the vectors.  Cosine distance has a range of [0, 2].

        Note: the cosine distance is undefined when one (or both) of the vectors
        are all zeros (there is no direction).  These vectors are invalid and may
        never be returned from a vector search.

        "dot" - Dot product. Dot distance is the dot product of two vectors. Dot
        distance has a range of (-∞, ∞). If the vectors are normalized (i.e. their
        l2 norm is 1), then dot distance is equivalent to the cosine distance.
    num_partitions: int, default sqrt(num_rows)
        The number of IVF partitions to create.

        This value should generally scale with the number of rows in the dataset.
        By default the number of partitions is the square root of the number of
        rows.

        If this value is too large then the first part of the search (picking the
        right partition) will be slow.  If this value is too small then the second
        part of the search (searching within a partition) will be slow.
    num_sub_vectors: int, default is vector dimension / 16
        Number of sub-vectors of PQ.

        This value controls how much the vector is compressed during the
        quantization step.  The more sub vectors there are the less the vector is
        compressed.  The default is the dimension of the vector divided by 16.  If
        the dimension is not evenly divisible by 16 we use the dimension divded by
        8.

        The above two cases are highly preferred.  Having 8 or 16 values per
        subvector allows us to use efficient SIMD instructions.

        If the dimension is not visible by 8 then we use 1 subvector.  This is not
        ideal and will likely result in poor performance.
    num_bits: int, default 8
        Number of bits to encode each sub-vector.

        This value controls how much the sub-vectors are compressed.  The more bits
        the more accurate the index but the slower search.  The default is 8
        bits.  Only 4 and 8 are supported.
    max_iterations: int, default 50
        Max iteration to train kmeans.

        When training an IVF PQ index we use kmeans to calculate the partitions.
        This parameter controls how many iterations of kmeans to run.

        Increasing this might improve the quality of the index but in most cases
        these extra iterations have diminishing returns.

        The default value is 50.
    sample_rate: int, default 256
        The rate used to calculate the number of training vectors for kmeans.

        When an IVF PQ index is trained, we need to calculate partitions.  These
        are groups of vectors that are similar to each other.  To do this we use an
        algorithm called kmeans.

        Running kmeans on a large dataset can be slow.  To speed this up we run
        kmeans on a random sample of the data.  This parameter controls the size of
        the sample.  The total number of vectors used to train the index is
        `sample_rate * num_partitions`.

        Increasing this value might improve the quality of the index but in most
        cases the default should be sufficient.

        The default value is 256.
    """

    distance_type: Literal["l2", "cosine", "dot"] = "l2"
    num_partitions: Optional[int] = None
    num_sub_vectors: Optional[int] = None
    num_bits: int = 8
    max_iterations: int = 50
    sample_rate: int = 256


__all__ = [
    "BTree",
    "IvfPq",
    "IvfFlat",
    "HnswPq",
    "HnswSq",
    "IndexConfig",
    "FTS",
    "Bitmap",
    "LabelList",
]
