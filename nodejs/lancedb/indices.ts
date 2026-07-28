// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import { Index as LanceDbIndex } from "./native";
import type { Table } from "./table";

/**
 * Options to create an `IVF_PQ` index
 */
export interface IvfPqOptions {
  /**
   * The number of IVF partitions to create.
   *
   * This value should generally scale with the number of rows in the dataset.
   * By default the number of partitions is the square root of the number of
   * rows.
   *
   * If this value is too large then the first part of the search (picking the
   * right partition) will be slow.  If this value is too small then the second
   * part of the search (searching within a partition) will be slow.
   */
  numPartitions?: number;

  /**
   * Number of sub-vectors of PQ.
   *
   * This value controls how much the vector is compressed during the quantization step.
   * The more sub vectors there are the less the vector is compressed.  The default is
   * the dimension of the vector divided by 16.  If the dimension is not evenly divisible
   * by 16 we use the dimension divded by 8.
   *
   * The above two cases are highly preferred.  Having 8 or 16 values per subvector allows
   * us to use efficient SIMD instructions.
   *
   * If the dimension is not visible by 8 then we use 1 subvector.  This is not ideal and
   * will likely result in poor performance.
   */
  numSubVectors?: number;

  /**
   * Number of bits per sub-vector.
   *
   * This value controls how much each subvector is compressed.  The more bits the more
   * accurate the index will be but the slower search.  The default is 8 bits.
   *
   * The number of bits must be 4 or 8.
   */
  numBits?: number;

  /**
   * Distance type to use to build the index.
   *
   * Default value is "l2".
   *
   * This is used when training the index to calculate the IVF partitions
   * (vectors are grouped in partitions with similar vectors according to this
   * distance type) and to calculate a subvector's code during quantization.
   *
   * The distance type used to train an index MUST match the distance type used
   * to search the index.  Failure to do so will yield inaccurate results.
   *
   * The following distance types are available:
   *
   * "l2" - Euclidean distance. This is a very common distance metric that
   * accounts for both magnitude and direction when determining the distance
   * between vectors. l2 distance has a range of [0, ∞).
   *
   * "cosine" - Cosine distance.  Cosine distance is a distance metric
   * calculated from the cosine similarity between two vectors. Cosine
   * similarity is a measure of similarity between two non-zero vectors of an
   * inner product space. It is defined to equal the cosine of the angle
   * between them.  Unlike l2, the cosine distance is not affected by the
   * magnitude of the vectors.  Cosine distance has a range of [0, 2].
   *
   * Note: the cosine distance is undefined when one (or both) of the vectors
   * are all zeros (there is no direction).  These vectors are invalid and may
   * never be returned from a vector search.
   *
   * "dot" - Dot product. Dot distance is the dot product of two vectors. Dot
   * distance has a range of (-∞, ∞). If the vectors are normalized (i.e. their
   * l2 norm is 1), then dot distance is equivalent to the cosine distance.
   */
  distanceType?: "l2" | "cosine" | "dot";

  /**
   * Max iteration to train IVF kmeans.
   *
   * When training an IVF PQ index we use kmeans to calculate the partitions.  This parameter
   * controls how many iterations of kmeans to run.
   *
   * Increasing this might improve the quality of the index but in most cases these extra
   * iterations have diminishing returns.
   *
   * The default value is 50.
   */
  maxIterations?: number;

  /**
   * The number of vectors, per partition, to sample when training IVF kmeans.
   *
   * When an IVF PQ index is trained, we need to calculate partitions.  These are groups
   * of vectors that are similar to each other.  To do this we use an algorithm called kmeans.
   *
   * Running kmeans on a large dataset can be slow.  To speed this up we run kmeans on a
   * random sample of the data.  This parameter controls the size of the sample.  The total
   * number of vectors used to train the index is `sample_rate * num_partitions`.
   *
   * Increasing this value might improve the quality of the index but in most cases the
   * default should be sufficient.
   *
   * The default value is 256.
   */
  sampleRate?: number;
}

export interface IvfRqOptions {
  /**
   * The number of IVF partitions to create.
   *
   * This value should generally scale with the number of rows in the dataset.
   * By default the number of partitions is the square root of the number of
   * rows.
   *
   * If this value is too large then the first part of the search (picking the
   * right partition) will be slow. If this value is too small then the second
   * part of the search (searching within a partition) will be slow.
   */
  numPartitions?: number;

  /**
   * Number of bits per dimension for residual quantization.
   *
   * This value controls how much each residual component is compressed. The more
   * bits, the more accurate the index will be but the slower search. Typical values
   * are small integers; the default is 1 bit per dimension.
   */
  numBits?: number;

  /**
   * Distance type to use to build the index.
   *
   * Default value is "l2".
   *
   * This is used when training the index to calculate the IVF partitions
   * (vectors are grouped in partitions with similar vectors according to this
   * distance type) and during quantization.
   *
   * The distance type used to train an index MUST match the distance type used
   * to search the index. Failure to do so will yield inaccurate results.
   *
   * The following distance types are available:
   *
   * "l2" - Euclidean distance.
   * "cosine" - Cosine distance.
   * "dot" - Dot product.
   */
  distanceType?: "l2" | "cosine" | "dot";

  /**
   * Max iterations to train IVF kmeans.
   *
   * When training an IVF index we use kmeans to calculate the partitions. This parameter
   * controls how many iterations of kmeans to run.
   *
   * The default value is 50.
   */
  maxIterations?: number;

  /**
   * The number of vectors, per partition, to sample when training IVF kmeans.
   *
   * When an IVF index is trained, we need to calculate partitions. These are groups
   * of vectors that are similar to each other. To do this we use an algorithm called kmeans.
   *
   * Running kmeans on a large dataset can be slow. To speed this up we run kmeans on a
   * random sample of the data. This parameter controls the size of the sample. The total
   * number of vectors used to train the index is `sample_rate * num_partitions`.
   *
   * Increasing this value might improve the quality of the index but in most cases the
   * default should be sufficient.
   *
   * The default value is 256.
   */
  sampleRate?: number;
}

/**
 * Options to create an `HNSW_PQ` index
 */
export interface HnswPqOptions {
  /**
   * The distance metric used to train the index.
   *
   * Default value is "l2".
   *
   * The following distance types are available:
   *
   * "l2" - Euclidean distance. This is a very common distance metric that
   * accounts for both magnitude and direction when determining the distance
   * between vectors. l2 distance has a range of [0, ∞).
   *
   * "cosine" - Cosine distance.  Cosine distance is a distance metric
   * calculated from the cosine similarity between two vectors. Cosine
   * similarity is a measure of similarity between two non-zero vectors of an
   * inner product space. It is defined to equal the cosine of the angle
   * between them.  Unlike l2, the cosine distance is not affected by the
   * magnitude of the vectors.  Cosine distance has a range of [0, 2].
   *
   * "dot" - Dot product. Dot distance is the dot product of two vectors. Dot
   * distance has a range of (-∞, ∞). If the vectors are normalized (i.e. their
   * l2 norm is 1), then dot distance is equivalent to the cosine distance.
   */
  distanceType?: "l2" | "cosine" | "dot";

  /**
   * The number of IVF partitions to create.
   *
   * For HNSW, we recommend a small number of partitions. Setting this to 1 works
   * well for most tables. For very large tables, training just one HNSW graph
   * will require too much memory. Each partition becomes its own HNSW graph, so
   * setting this value higher reduces the peak memory use of training.
   *
   */
  numPartitions?: number;

  /**
   * Number of sub-vectors of PQ.
   *
   * This value controls how much the vector is compressed during the quantization step.
   * The more sub vectors there are the less the vector is compressed.  The default is
   * the dimension of the vector divided by 16.  If the dimension is not evenly divisible
   * by 16 we use the dimension divded by 8.
   *
   * The above two cases are highly preferred.  Having 8 or 16 values per subvector allows
   * us to use efficient SIMD instructions.
   *
   * If the dimension is not visible by 8 then we use 1 subvector.  This is not ideal and
   * will likely result in poor performance.
   *
   */
  numSubVectors?: number;

  /**
   * Max iterations to train kmeans.
   *
   * The default value is 50.
   *
   * When training an IVF index we use kmeans to calculate the partitions.  This parameter
   * controls how many iterations of kmeans to run.
   *
   * Increasing this might improve the quality of the index but in most cases the parameter
   * is unused because kmeans will converge with fewer iterations.  The parameter is only
   * used in cases where kmeans does not appear to converge.  In those cases it is unlikely
   * that setting this larger will lead to the index converging anyways.
   *
   */
  maxIterations?: number;

  /**
   * The rate used to calculate the number of training vectors for kmeans.
   *
   * Default value is 256.
   *
   * When an IVF index is trained, we need to calculate partitions.  These are groups
   * of vectors that are similar to each other.  To do this we use an algorithm called kmeans.
   *
   * Running kmeans on a large dataset can be slow.  To speed this up we run kmeans on a
   * random sample of the data.  This parameter controls the size of the sample.  The total
   * number of vectors used to train the index is `sample_rate * num_partitions`.
   *
   * Increasing this value might improve the quality of the index but in most cases the
   * default should be sufficient.
   *
   */
  sampleRate?: number;

  /**
   * The number of neighbors to select for each vector in the HNSW graph.
   *
   * The default value is 20.
   *
   * This value controls the tradeoff between search speed and accuracy.
   * The higher the value the more accurate the search but the slower it will be.
   *
   */
  m?: number;

  /**
   * The number of candidates to evaluate during the construction of the HNSW graph.
   *
   * The default value is 300.
   *
   * This value controls the tradeoff between build speed and accuracy.
   * The higher the value the more accurate the build but the slower it will be.
   * 150 to 300 is the typical range. 100 is a minimum for good quality search
   * results. In most cases, there is no benefit to setting this higher than 500.
   * This value should be set to a value that is not less than `ef` in the search phase.
   *
   */
  efConstruction?: number;
}

/**
 * Options to create an `HNSW_SQ` index
 */
export interface HnswSqOptions {
  /**
   * The distance metric used to train the index.
   *
   * Default value is "l2".
   *
   * The following distance types are available:
   *
   * "l2" - Euclidean distance. This is a very common distance metric that
   * accounts for both magnitude and direction when determining the distance
   * between vectors. l2 distance has a range of [0, ∞).
   *
   * "cosine" - Cosine distance.  Cosine distance is a distance metric
   * calculated from the cosine similarity between two vectors. Cosine
   * similarity is a measure of similarity between two non-zero vectors of an
   * inner product space. It is defined to equal the cosine of the angle
   * between them.  Unlike l2, the cosine distance is not affected by the
   * magnitude of the vectors.  Cosine distance has a range of [0, 2].
   *
   * "dot" - Dot product. Dot distance is the dot product of two vectors. Dot
   * distance has a range of (-∞, ∞). If the vectors are normalized (i.e. their
   * l2 norm is 1), then dot distance is equivalent to the cosine distance.
   */
  distanceType?: "l2" | "cosine" | "dot";

  /**
   * The number of IVF partitions to create.
   *
   * For HNSW, we recommend a small number of partitions. Setting this to 1 works
   * well for most tables. For very large tables, training just one HNSW graph
   * will require too much memory. Each partition becomes its own HNSW graph, so
   * setting this value higher reduces the peak memory use of training.
   *
   */
  numPartitions?: number;

  /**
   * Max iterations to train kmeans.
   *
   * The default value is 50.
   *
   * When training an IVF index we use kmeans to calculate the partitions.  This parameter
   * controls how many iterations of kmeans to run.
   *
   * Increasing this might improve the quality of the index but in most cases the parameter
   * is unused because kmeans will converge with fewer iterations.  The parameter is only
   * used in cases where kmeans does not appear to converge.  In those cases it is unlikely
   * that setting this larger will lead to the index converging anyways.
   *
   */
  maxIterations?: number;

  /**
   * The rate used to calculate the number of training vectors for kmeans.
   *
   * Default value is 256.
   *
   * When an IVF index is trained, we need to calculate partitions.  These are groups
   * of vectors that are similar to each other.  To do this we use an algorithm called kmeans.
   *
   * Running kmeans on a large dataset can be slow.  To speed this up we run kmeans on a
   * random sample of the data.  This parameter controls the size of the sample.  The total
   * number of vectors used to train the index is `sample_rate * num_partitions`.
   *
   * Increasing this value might improve the quality of the index but in most cases the
   * default should be sufficient.
   *
   */
  sampleRate?: number;

  /**
   * The number of neighbors to select for each vector in the HNSW graph.
   *
   * The default value is 20.
   *
   * This value controls the tradeoff between search speed and accuracy.
   * The higher the value the more accurate the search but the slower it will be.
   *
   */
  m?: number;

  /**
   * The number of candidates to evaluate during the construction of the HNSW graph.
   *
   * The default value is 300.
   *
   * This value controls the tradeoff between build speed and accuracy.
   * The higher the value the more accurate the build but the slower it will be.
   * 150 to 300 is the typical range. 100 is a minimum for good quality search
   * results. In most cases, there is no benefit to setting this higher than 500.
   * This value should be set to a value that is not less than `ef` in the search phase.
   *
   */
  efConstruction?: number;
}

/**
 * Options to create an `IVF_FLAT` index
 */
export interface IvfFlatOptions {
  /**
   * The number of IVF partitions to create.
   *
   * This value should generally scale with the number of rows in the dataset.
   * By default the number of partitions is the square root of the number of
   * rows.
   *
   * If this value is too large then the first part of the search (picking the
   * right partition) will be slow.  If this value is too small then the second
   * part of the search (searching within a partition) will be slow.
   */
  numPartitions?: number;

  /**
   * Distance type to use to build the index.
   *
   * Default value is "l2".
   *
   * This is used when training the index to calculate the IVF partitions
   * (vectors are grouped in partitions with similar vectors according to this
   * distance type).
   *
   * The distance type used to train an index MUST match the distance type used
   * to search the index.  Failure to do so will yield inaccurate results.
   *
   * The following distance types are available:
   *
   * "l2" - Euclidean distance. This is a very common distance metric that
   * accounts for both magnitude and direction when determining the distance
   * between vectors. l2 distance has a range of [0, ∞).
   *
   * "cosine" - Cosine distance.  Cosine distance is a distance metric
   * calculated from the cosine similarity between two vectors. Cosine
   * similarity is a measure of similarity between two non-zero vectors of an
   * inner product space. It is defined to equal the cosine of the angle
   * between them.  Unlike l2, the cosine distance is not affected by the
   * magnitude of the vectors.  Cosine distance has a range of [0, 2].
   *
   * Note: the cosine distance is undefined when one (or both) of the vectors
   * are all zeros (there is no direction).  These vectors are invalid and may
   * never be returned from a vector search.
   *
   * "dot" - Dot product. Dot distance is the dot product of two vectors. Dot
   * distance has a range of (-∞, ∞). If the vectors are normalized (i.e. their
   * l2 norm is 1), then dot distance is equivalent to the cosine distance.
   *
   * "hamming" - Hamming distance. Hamming distance is a distance metric
   * calculated from the number of bits that are different between two vectors.
   * Hamming distance has a range of [0, dimension]. Note that the hamming distance
   * is only valid for binary vectors.
   */
  distanceType?: "l2" | "cosine" | "dot" | "hamming";

  /**
   * Max iteration to train IVF kmeans.
   *
   * When training an IVF FLAT index we use kmeans to calculate the partitions.  This parameter
   * controls how many iterations of kmeans to run.
   *
   * Increasing this might improve the quality of the index but in most cases these extra
   * iterations have diminishing returns.
   *
   * The default value is 50.
   */
  maxIterations?: number;

  /**
   * The number of vectors, per partition, to sample when training IVF kmeans.
   *
   * When an IVF FLAT index is trained, we need to calculate partitions.  These are groups
   * of vectors that are similar to each other.  To do this we use an algorithm called kmeans.
   *
   * Running kmeans on a large dataset can be slow.  To speed this up we run kmeans on a
   * random sample of the data.  This parameter controls the size of the sample.  The total
   * number of vectors used to train the index is `sample_rate * num_partitions`.
   *
   * Increasing this value might improve the quality of the index but in most cases the
   * default should be sufficient.
   *
   * The default value is 256.
   */
  sampleRate?: number;
}

export type BaseTokenizer =
  | "simple"
  | "whitespace"
  | "raw"
  | "ngram"
  | "icu"
  | "icu/split"
  | `jieba/${string}`
  | `lindera/${string}`;

/**
 * A source for custom full-text-search stop words.
 *
 * An inline array supplies the entries directly. A file is read as UTF-8 with
 * one stop word per line. A table source reads stop words from the selected
 * string column of a local/native LanceDB table. Remote table sources are
 * rejected because the client cannot currently guarantee a complete snapshot.
 *
 * ```ts
 * const inline: CustomStopWordsSource = ["copyright", "reserved"];
 * const file: CustomStopWordsSource = {
 *   source: "file",
 *   path: "./stop-words.txt",
 * };
 * const tableColumn: CustomStopWordsSource = {
 *   source: "table",
 *   table: stopWordsTable,
 *   column: "word",
 * };
 * ```
 *
 * Empty strings are ignored and exact duplicates are removed while preserving
 * the first occurrence. Values are otherwise preserved exactly: LanceDB does
 * not trim them, lowercase them, or otherwise normalize their contents.
 * Embedded/local fuzzy queries fail closed when `fuzziness` is greater than
 * zero and a custom snapshot is active; `fuzziness: 0` and indexes without a
 * custom snapshot continue to work normally. Remote tables currently reject
 * every explicit `fuzziness > 0` query because the server protocol does not
 * declare tokenizer-snapshot-safe fuzzy search; omit `fuzziness` or use
 * `fuzziness: 0`.
 *
 * The source is resolved when the index is created, and the resulting list is
 * stored as a stable index snapshot. Standalone `tokenize` resolves the same
 * kind of one-call snapshot. File paths are always read by this client, never
 * by a remote LanceDB service.
 */
export type CustomStopWordsSource =
  | string[]
  | FtsStopWordsFileSource
  | FtsStopWordsTableSource;

/** A newline-delimited UTF-8 custom stop-words file on this client. */
export interface FtsStopWordsFileSource {
  /** Select a newline-delimited UTF-8 file. */
  source: "file";
  /** Path to the stop-words file on the client. */
  path: string;
}

/** A custom stop-words snapshot read from a LanceDB table column. */
export interface FtsStopWordsTableSource {
  /** Select a LanceDB table column. */
  source: "table";
  /**
   * Local/native table containing the stop words.
   *
   * A remote table cannot be used as the source. Materialize its stop-word
   * column locally first, or use an inline list or UTF-8 file.
   */
  table: Table;
  /** Name of the string column containing the stop words. */
  column: string;
}

/**
 * Validate and copy a custom-stop-words source without resolving it.
 *
 * @internal
 */
export function normalizeCustomStopWordsSource(
  value: unknown,
): CustomStopWordsSource | undefined {
  if (value === undefined) {
    return undefined;
  }

  if (Array.isArray(value)) {
    for (const [index, stopWord] of value.entries()) {
      if (typeof stopWord !== "string") {
        throw new TypeError(
          `customStopWords[${index}] must be a string, received ${typeof stopWord}`,
        );
      }
    }
    // Keep [] distinct from undefined and prevent later caller mutation.
    return [...value];
  }

  if (value === null || typeof value !== "object") {
    throw new TypeError(
      "customStopWords must be a string array, a file source, or a table source",
    );
  }

  const candidate = value as Record<string, unknown>;
  if (candidate.source === "file") {
    if (candidate.table !== undefined || candidate.column !== undefined) {
      throw new TypeError(
        "customStopWords file and table sources are mutually exclusive",
      );
    }
    if (typeof candidate.path !== "string" || candidate.path.length === 0) {
      throw new TypeError(
        "customStopWords file source requires a non-empty string 'path'",
      );
    }
    return { source: "file", path: candidate.path };
  }

  if (candidate.source === "table") {
    if (candidate.path !== undefined) {
      throw new TypeError(
        "customStopWords file and table sources are mutually exclusive",
      );
    }
    if (candidate.table === null || typeof candidate.table !== "object") {
      throw new TypeError(
        "customStopWords table source requires a LanceDB 'table'",
      );
    }
    if (typeof candidate.column !== "string" || candidate.column.length === 0) {
      throw new TypeError(
        "customStopWords table source requires a non-empty string 'column'",
      );
    }
    return {
      source: "table",
      table: candidate.table as Table,
      column: candidate.column,
    };
  }

  throw new TypeError(
    "customStopWords object source must have source: 'file' or source: 'table'",
  );
}

/**
 * Options to create a full text search index
 */
export interface FtsOptions {
  /**
   * Whether to build the index with positions.
   * True by default.
   * If set to false, the index will not store the positions of the tokens in the text,
   * which will make the index smaller and faster to build, but will not support phrase queries.
   */
  withPosition?: boolean;

  /**
   * The tokenizer to use when building the index.
   * The default is "simple".
   *
   * The following tokenizers are available:
   *
   * "simple" - Simple tokenizer. This tokenizer splits the text into tokens using whitespace and punctuation as a delimiter.
   *
   * "whitespace" - Whitespace tokenizer. This tokenizer splits the text into tokens using whitespace as a delimiter.
   *
   * "raw" - Raw tokenizer. This tokenizer does not split the text into tokens and indexes the entire text as a single token.
   *
   * "icu" - ICU dictionary-based word segmentation.
   *
   * "icu/split" - ICU segmentation with simple-style delimiter splitting.
   */
  baseTokenizer?: BaseTokenizer;

  /**
   * language for stemming and stop words
   * this is only used when `stem` or `remove_stop_words` is true
   */
  language?: string;

  /**
   * maximum token length
   * tokens longer than this length will be ignored
   */
  maxTokenLength?: number;

  /**
   * whether to lowercase tokens
   */
  lowercase?: boolean;

  /**
   * whether to stem tokens
   */
  stem?: boolean;

  /**
   * whether to remove stop words
   */
  removeStopWords?: boolean;

  /**
   * Custom stop words that replace the built-in list for `language`.
   *
   * This option only affects tokenization when `removeStopWords` is true. The
   * source can be an inline string array, a newline-delimited UTF-8 file on
   * this client, or a string column from a local/native LanceDB table.
   * Remote table sources are rejected because they cannot currently guarantee
   * a complete snapshot; the target table may still be remote.
   *
   * `undefined` keeps the built-in language list. An empty array explicitly
   * replaces it with no stop words.
   *
   * Embedded/local fuzzy queries with `fuzziness > 0` are rejected while this
   * snapshot and `removeStopWords` are active. Use `fuzziness: 0`, or omit the
   * custom snapshot. Remote tables reject every explicit `fuzziness > 0`
   * query, regardless of stop-word configuration, because the server protocol
   * does not declare tokenizer-snapshot-safe fuzzy search; omit `fuzziness` or
   * use `fuzziness: 0`.
   */
  customStopWords?: CustomStopWordsSource;

  /**
   * whether to remove punctuation
   */
  asciiFolding?: boolean;

  /**
   * ngram min length
   */
  ngramMinLength?: number;

  /**
   * ngram max length
   */
  ngramMaxLength?: number;

  /**
   * whether to only index the prefix of the token for ngram tokenizer
   */
  prefixOnly?: boolean;

  /**
   * Number of documents per compressed posting block.
   *
   * The default is 128. Supported values are 128 and 256. A value of 256 uses
   * the experimental FTS V3 format and may introduce breaking changes.
   */
  blockSize?: 128 | 256;
}

export class Index {
  private readonly inner?: LanceDbIndex;
  private readonly ftsOptions?: Readonly<Partial<FtsOptions>>;
  private constructor(
    inner?: LanceDbIndex,
    ftsOptions?: Readonly<Partial<FtsOptions>>,
  ) {
    this.inner = inner;
    this.ftsOptions = ftsOptions;
  }

  /**
   * Create an IvfPq index
   *
   * This index stores a compressed (quantized) copy of every vector.  These vectors
   * are grouped into partitions of similar vectors.  Each partition keeps track of
   * a centroid which is the average value of all vectors in the group.
   *
   * During a query the centroids are compared with the query vector to find the closest
   * partitions.  The compressed vectors in these partitions are then searched to find
   * the closest vectors.
   *
   * The compression scheme is called product quantization.  Each vector is divided into
   * subvectors and then each subvector is quantized into a small number of bits.  the
   * parameters `num_bits` and `num_subvectors` control this process, providing a tradeoff
   * between index size (and thus search speed) and index accuracy.
   *
   * The partitioning process is called IVF and the `num_partitions` parameter controls how
   * many groups to create.
   *
   * Note that training an IVF PQ index on a large dataset is a slow operation and
   * currently is also a memory intensive operation.
   */
  static ivfPq(options?: Partial<IvfPqOptions>) {
    return new Index(
      LanceDbIndex.ivfPq(
        options?.distanceType,
        options?.numPartitions,
        options?.numSubVectors,
        options?.numBits,
        options?.maxIterations,
        options?.sampleRate,
      ),
    );
  }

  /**
   * Create an IvfRq index
   *
   * IVF-RQ (RabitQ Quantization) compresses vectors using RabitQ quantization
   * and organizes them into IVF partitions.
   *
   * The compression scheme is called RabitQ quantization. Each dimension is quantized into a small number of bits.
   * The parameters `num_bits` and `num_partitions` control this process, providing a tradeoff
   * between index size (and thus search speed) and index accuracy.
   *
   * The partitioning process is called IVF and the `num_partitions` parameter controls how
   * many groups to create.
   *
   * Note that training an IVF RQ index on a large dataset is a slow operation and
   * currently is also a memory intensive operation.
   */
  static ivfRq(options?: Partial<IvfRqOptions>) {
    return new Index(
      LanceDbIndex.ivfRq(
        options?.distanceType,
        options?.numPartitions,
        options?.numBits,
        options?.maxIterations,
        options?.sampleRate,
      ),
    );
  }

  /**
   * Create an IvfFlat index
   *
   * This index groups vectors into partitions of similar vectors.  Each partition keeps track of
   * a centroid which is the average value of all vectors in the group.
   *
   * During a query the centroids are compared with the query vector to find the closest
   * partitions.  The vectors in these partitions are then searched to find
   * the closest vectors.
   *
   * The partitioning process is called IVF and the `num_partitions` parameter controls how
   * many groups to create.
   *
   * Note that training an IVF FLAT index on a large dataset is a slow operation and
   * currently is also a memory intensive operation.
   */
  static ivfFlat(options?: Partial<IvfFlatOptions>) {
    return new Index(
      LanceDbIndex.ivfFlat(
        options?.distanceType,
        options?.numPartitions,
        options?.maxIterations,
        options?.sampleRate,
      ),
    );
  }

  /**
   * Create a btree index
   *
   * A btree index is an index on a scalar columns.  The index stores a copy of the column
   * in sorted order.  A header entry is created for each block of rows (currently the
   * block size is fixed at 4096).  These header entries are stored in a separate
   * cacheable structure (a btree).  To search for data the header is used to determine
   * which blocks need to be read from disk.
   *
   * For example, a btree index in a table with 1Bi rows requires sizeof(Scalar) * 256Ki
   * bytes of memory and will generally need to read sizeof(Scalar) * 4096 bytes to find
   * the correct row ids.
   *
   * This index is good for scalar columns with mostly distinct values and does best when
   * the query is highly selective.
   *
   * The btree index does not currently have any parameters though parameters such as the
   * block size may be added in the future.
   */
  static btree() {
    return new Index(LanceDbIndex.btree());
  }

  /**
   * Create a bitmap index.
   *
   * A `Bitmap` index stores a bitmap for each distinct value in the column for every row.
   *
   * This index works best for low-cardinality columns, where the number of unique values
   * is small (i.e., less than a few hundreds).
   */
  static bitmap() {
    return new Index(LanceDbIndex.bitmap());
  }

  /**
   * Create a label list index.
   *
   * LabelList index is a scalar index that can be used on `List<T>` columns to
   * support queries with `array_contains_all` and `array_contains_any`
   * using an underlying bitmap index.
   */
  static labelList() {
    return new Index(LanceDbIndex.labelList());
  }

  /**
   * Create an FM-Index.
   *
   * An FM-Index is a scalar index on string or binary columns that accelerates
   * substring search, i.e. `contains(col, 'needle')`. Unlike the tokenized
   * full-text-search index, it matches arbitrary substrings of the raw bytes.
   */
  static fm() {
    return new Index(LanceDbIndex.fm());
  }

  /**
   * Create a full text search index
   *
   * A full text search index is an index on a string column, so that you can conduct full
   * text searches on the column.
   *
   * The results of a full text search are ordered by relevance measured by BM25.
   *
   * You can combine filters with full text search.
   *
   * @example
   * Use an inline stop-word snapshot:
   * ```ts
   * await table.createIndex("text", {
   *   config: Index.fts({
   *     removeStopWords: true,
   *     customStopWords: ["copyright", "reserved"],
   *   }),
   * });
   * ```
   *
   * @example
   * Read a newline-delimited UTF-8 file on the client:
   * ```ts
   * await table.createIndex("text", {
   *   config: Index.fts({
   *     removeStopWords: true,
   *     customStopWords: { source: "file", path: "./stop-words.txt" },
   *   }),
   * });
   * ```
   *
   * @example
   * Snapshot a string column from another local/native LanceDB table:
   * ```ts
   * await table.createIndex("text", {
   *   config: Index.fts({
   *     removeStopWords: true,
   *     customStopWords: {
   *       source: "table",
   *       table: stopWordsTable,
   *       column: "word",
   *     },
   *   }),
   * });
   * ```
   */
  static fts(options?: Partial<FtsOptions>) {
    const customStopWords = normalizeCustomStopWordsSource(
      options?.customStopWords,
    );
    if (
      options?.blockSize !== undefined &&
      options.blockSize !== 128 &&
      options.blockSize !== 256
    ) {
      throw new RangeError("FTS blockSize must be 128 or 256");
    }
    // Preserve the synchronous validation behavior of Index.fts while
    // discarding the single-use native builder. Table.createIndex constructs a
    // fresh builder from the saved recipe below.
    LanceDbIndex.fts(
      options?.withPosition,
      options?.baseTokenizer,
      options?.language,
      options?.maxTokenLength,
      options?.lowercase,
      options?.stem,
      options?.removeStopWords,
      options?.asciiFolding,
      options?.ngramMinLength,
      options?.ngramMaxLength,
      options?.prefixOnly,
      options?.blockSize,
    );
    // File and table sources can only be resolved asynchronously when
    // Table.createIndex runs. Save an immutable recipe and create a fresh
    // native builder for each invocation so failed resolution does not consume
    // the public Index configuration.
    return new Index(undefined, {
      ...options,
      customStopWords,
    });
  }

  /**
   *
   * Create a hnswPq index
   *
   * HNSW-PQ stands for Hierarchical Navigable Small World - Product Quantization.
   * It is a variant of the HNSW algorithm that uses product quantization to compress
   * the vectors.
   *
   */
  static hnswPq(options?: Partial<HnswPqOptions>) {
    return new Index(
      LanceDbIndex.hnswPq(
        options?.distanceType,
        options?.numPartitions,
        options?.numSubVectors,
        options?.maxIterations,
        options?.sampleRate,
        options?.m,
        options?.efConstruction,
      ),
    );
  }

  /**
   *
   * Create a hnswSq index
   *
   * HNSW-SQ stands for Hierarchical Navigable Small World - Scalar Quantization.
   * It is a variant of the HNSW algorithm that uses scalar quantization to compress
   * the vectors.
   *
   */
  static hnswSq(options?: Partial<HnswSqOptions>) {
    return new Index(
      LanceDbIndex.hnswSq(
        options?.distanceType,
        options?.numPartitions,
        options?.maxIterations,
        options?.sampleRate,
        options?.m,
        options?.efConstruction,
      ),
    );
  }
}

export interface IndexOptions {
  /**
   * Advanced index configuration
   *
   * This option allows you to specify a specfic index to create and also
   * allows you to pass in configuration for training the index.
   *
   * See the static methods on Index for details on the various index types.
   *
   * If this is not supplied then column data type(s) and column statistics
   * will be used to determine the most useful kind of index to create.
   */
  config?: Index;
  /**
   * Whether to replace the existing index
   *
   * If this is false, and another index already exists on the same columns
   * and the same name, then an error will be returned.  This is true even if
   * that index is out of date.
   *
   * The default is true
   */
  replace?: boolean;

  /**
   * Timeout in seconds to wait for index creation to complete.
   *
   * If not specified, the method will return immediately after starting the index creation.
   */
  waitTimeoutSeconds?: number;

  /**
   * Optional custom name for the index.
   *
   * If not provided, a default name will be generated based on the column name.
   */
  name?: string;

  /**
   * Whether to train the index with existing data.
   *
   * If true (default), the index will be trained with existing data in the table.
   * If false, the index will be created empty and populated as new data is added.
   *
   * Note: This option is only supported for scalar indices. Vector indices always train.
   */
  train?: boolean;
}
