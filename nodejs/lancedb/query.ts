// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import {
  Table as ArrowTable,
  type IntoVector,
  RecordBatch,
  fromBufferToRecordBatch,
  fromRecordBatchToBuffer,
  tableFromIPC,
} from "./arrow";
import { type IvfPqOptions } from "./indices";
import {
  RecordBatchIterator as NativeBatchIterator,
  Query as NativeQuery,
  Table as NativeTable,
  VectorQuery as NativeVectorQuery,
} from "./native";
import { Reranker } from "./rerankers";

export class RecordBatchIterator implements AsyncIterator<RecordBatch> {
  private promisedInner?: Promise<NativeBatchIterator>;
  private inner?: NativeBatchIterator;

  constructor(promise?: Promise<NativeBatchIterator>) {
    // TODO: check promise reliably so we dont need to pass two arguments.
    this.promisedInner = promise;
  }

  // biome-ignore lint/suspicious/noExplicitAny: skip
  async next(): Promise<IteratorResult<RecordBatch<any>>> {
    if (this.inner === undefined) {
      this.inner = await this.promisedInner;
    }
    if (this.inner === undefined) {
      throw new Error("Invalid iterator state state");
    }
    const n = await this.inner.next();
    if (n == null) {
      return Promise.resolve({ done: true, value: null });
    }
    const tbl = tableFromIPC(n);
    if (tbl.batches.length != 1) {
      throw new Error("Expected only one batch");
    }
    return Promise.resolve({ done: false, value: tbl.batches[0] });
  }
}
/* eslint-enable */

class RecordBatchIterable<
  NativeQueryType extends NativeQuery | NativeVectorQuery,
> implements AsyncIterable<RecordBatch>
{
  private inner: NativeQueryType;
  private options?: QueryExecutionOptions;

  constructor(inner: NativeQueryType, options?: QueryExecutionOptions) {
    this.inner = inner;
    this.options = options;
  }

  // biome-ignore lint/suspicious/noExplicitAny: skip
  [Symbol.asyncIterator](): AsyncIterator<RecordBatch<any>, any, undefined> {
    return new RecordBatchIterator(
      this.inner.execute(this.options?.maxBatchLength),
    );
  }
}

/**
 * Options that control the behavior of a particular query execution
 */
export interface QueryExecutionOptions {
  /**
   * The maximum number of rows to return in a single batch
   *
   * Batches may have fewer rows if the underlying data is stored
   * in smaller chunks.
   */
  maxBatchLength?: number;
}

/**
 * Options that control the behavior of a full text search
 */
export interface FullTextSearchOptions {
  /**
   * The columns to search
   *
   * If not specified, all indexed columns will be searched.
   * For now, only one column can be searched.
   */
  columns?: string | string[];
}

/** Common methods supported by all query types
 *
 * @see {@link Query}
 * @see {@link VectorQuery}
 *
 * @hideconstructor
 */
export class QueryBase<NativeQueryType extends NativeQuery | NativeVectorQuery>
  implements AsyncIterable<RecordBatch>
{
  /**
   * @hidden
   */
  protected constructor(
    protected inner: NativeQueryType | Promise<NativeQueryType>,
  ) {
    // intentionally empty
  }

  // call a function on the inner (either a promise or the actual object)
  /**
   * @hidden
   */
  protected doCall(fn: (inner: NativeQueryType) => void) {
    if (this.inner instanceof Promise) {
      this.inner = this.inner.then((inner) => {
        fn(inner);
        return inner;
      });
    } else {
      fn(this.inner);
    }
  }
  /**
   * A filter statement to be applied to this query.
   *
   * The filter should be supplied as an SQL query string.  For example:
   * @example
   * x > 10
   * y > 0 AND y < 100
   * x > 5 OR y = 'test'
   *
   * Filtering performance can often be improved by creating a scalar index
   * on the filter column(s).
   */
  where(predicate: string): this {
    this.doCall((inner: NativeQueryType) => inner.onlyIf(predicate));
    return this;
  }
  /**
   * A filter statement to be applied to this query.
   * @see where
   * @deprecated Use `where` instead
   */
  filter(predicate: string): this {
    return this.where(predicate);
  }

  fullTextSearch(
    query: string | FullTextQuery,
    options?: Partial<FullTextSearchOptions>,
  ): this {
    let columns: string[] | null = null;
    if (options) {
      if (typeof options.columns === "string") {
        columns = [options.columns];
      } else if (Array.isArray(options.columns)) {
        columns = options.columns;
      }
    }

    this.doCall((inner: NativeQueryType) => {
      if (typeof query === "string") {
        inner.fullTextSearch({
          query: query,
          columns: columns,
        });
      } else {
        // If query is a FullTextQuery object, convert it to a dict
        const queryObj = query.toDict();
        inner.fullTextSearch(queryObj);
      }
    });
    return this;
  }

  /**
   * Return only the specified columns.
   *
   * By default a query will return all columns from the table.  However, this can have
   * a very significant impact on latency.  LanceDb stores data in a columnar fashion.  This
   * means we can finely tune our I/O to select exactly the columns we need.
   *
   * As a best practice you should always limit queries to the columns that you need.  If you
   * pass in an array of column names then only those columns will be returned.
   *
   * You can also use this method to create new "dynamic" columns based on your existing columns.
   * For example, you may not care about "a" or "b" but instead simply want "a + b".  This is often
   * seen in the SELECT clause of an SQL query (e.g. `SELECT a+b FROM my_table`).
   *
   * To create dynamic columns you can pass in a Map<string, string>.  A column will be returned
   * for each entry in the map.  The key provides the name of the column.  The value is
   * an SQL string used to specify how the column is calculated.
   *
   * For example, an SQL query might state `SELECT a + b AS combined, c`.  The equivalent
   * input to this method would be:
   * @example
   * new Map([["combined", "a + b"], ["c", "c"]])
   *
   * Columns will always be returned in the order given, even if that order is different than
   * the order used when adding the data.
   *
   * Note that you can pass in a `Record<string, string>` (e.g. an object literal). This method
   * uses `Object.entries` which should preserve the insertion order of the object.  However,
   * object insertion order is easy to get wrong and `Map` is more foolproof.
   */
  select(
    columns: string[] | Map<string, string> | Record<string, string> | string,
  ): this {
    const selectColumns = (columnArray: string[]) => {
      this.doCall((inner: NativeQueryType) => {
        inner.selectColumns(columnArray);
      });
    };
    const selectMapping = (columnTuples: [string, string][]) => {
      this.doCall((inner: NativeQueryType) => {
        inner.select(columnTuples);
      });
    };

    if (typeof columns === "string") {
      selectColumns([columns]);
    } else if (Array.isArray(columns)) {
      selectColumns(columns);
    } else if (columns instanceof Map) {
      selectMapping(Array.from(columns.entries()));
    } else {
      selectMapping(Object.entries(columns));
    }

    return this;
  }

  /**
   * Set the maximum number of results to return.
   *
   * By default, a plain search has no limit.  If this method is not
   * called then every valid row from the table will be returned.
   */
  limit(limit: number): this {
    this.doCall((inner: NativeQueryType) => inner.limit(limit));
    return this;
  }

  offset(offset: number): this {
    this.doCall((inner: NativeQueryType) => inner.offset(offset));
    return this;
  }

  /**
   * Skip searching un-indexed data. This can make search faster, but will miss
   * any data that is not yet indexed.
   *
   * Use {@link Table#optimize} to index all un-indexed data.
   */
  fastSearch(): this {
    this.doCall((inner: NativeQueryType) => inner.fastSearch());
    return this;
  }

  /**
   * Whether to return the row id in the results.
   *
   * This column can be used to match results between different queries. For
   * example, to match results from a full text search and a vector search in
   * order to perform hybrid search.
   */
  withRowId(): this {
    this.doCall((inner: NativeQueryType) => inner.withRowId());
    return this;
  }

  /**
   * @hidden
   */
  protected nativeExecute(
    options?: Partial<QueryExecutionOptions>,
  ): Promise<NativeBatchIterator> {
    if (this.inner instanceof Promise) {
      return this.inner.then((inner) => inner.execute(options?.maxBatchLength));
    } else {
      return this.inner.execute(options?.maxBatchLength);
    }
  }

  /**
   * Execute the query and return the results as an @see {@link AsyncIterator}
   * of @see {@link RecordBatch}.
   *
   * By default, LanceDb will use many threads to calculate results and, when
   * the result set is large, multiple batches will be processed at one time.
   * This readahead is limited however and backpressure will be applied if this
   * stream is consumed slowly (this constrains the maximum memory used by a
   * single query)
   *
   */
  protected execute(
    options?: Partial<QueryExecutionOptions>,
  ): RecordBatchIterator {
    return new RecordBatchIterator(this.nativeExecute(options));
  }

  /**
   * @hidden
   */
  // biome-ignore lint/suspicious/noExplicitAny: skip
  [Symbol.asyncIterator](): AsyncIterator<RecordBatch<any>> {
    const promise = this.nativeExecute();
    return new RecordBatchIterator(promise);
  }

  /** Collect the results as an Arrow @see {@link ArrowTable}. */
  async toArrow(options?: Partial<QueryExecutionOptions>): Promise<ArrowTable> {
    const batches = [];
    let inner;
    if (this.inner instanceof Promise) {
      inner = await this.inner;
    } else {
      inner = this.inner;
    }
    for await (const batch of new RecordBatchIterable(inner, options)) {
      batches.push(batch);
    }
    return new ArrowTable(batches);
  }

  /** Collect the results as an array of objects. */
  // biome-ignore lint/suspicious/noExplicitAny: arrow.toArrow() returns any[]
  async toArray(options?: Partial<QueryExecutionOptions>): Promise<any[]> {
    const tbl = await this.toArrow(options);
    return tbl.toArray();
  }

  /**
   * Generates an explanation of the query execution plan.
   *
   * @example
   * import * as lancedb from "@lancedb/lancedb"
   * const db = await lancedb.connect("./.lancedb");
   * const table = await db.createTable("my_table", [
   *   { vector: [1.1, 0.9], id: "1" },
   * ]);
   * const plan = await table.query().nearestTo([0.5, 0.2]).explainPlan();
   *
   * @param verbose - If true, provides a more detailed explanation. Defaults to false.
   * @returns A Promise that resolves to a string containing the query execution plan explanation.
   */
  async explainPlan(verbose = false): Promise<string> {
    if (this.inner instanceof Promise) {
      return this.inner.then((inner) => inner.explainPlan(verbose));
    } else {
      return this.inner.explainPlan(verbose);
    }
  }

  /**
   * Executes the query and returns the physical query plan annotated with runtime metrics.
   *
   * This is useful for debugging and performance analysis, as it shows how the query was executed
   * and includes metrics such as elapsed time, rows processed, and I/O statistics.
   *
   * @example
   * import * as lancedb from "@lancedb/lancedb"
   *
   * const db = await lancedb.connect("./.lancedb");
   * const table = await db.createTable("my_table", [
   *   { vector: [1.1, 0.9], id: "1" },
   * ]);
   *
   * const plan = await table.query().nearestTo([0.5, 0.2]).analyzePlan();
   *
   * Example output (with runtime metrics inlined):
   * AnalyzeExec verbose=true, metrics=[]
   *  ProjectionExec: expr=[id@3 as id, vector@0 as vector, _distance@2 as _distance], metrics=[output_rows=1, elapsed_compute=3.292µs]
   *   Take: columns="vector, _rowid, _distance, (id)", metrics=[output_rows=1, elapsed_compute=66.001µs, batches_processed=1, bytes_read=8, iops=1, requests=1]
   *    CoalesceBatchesExec: target_batch_size=1024, metrics=[output_rows=1, elapsed_compute=3.333µs]
   *     GlobalLimitExec: skip=0, fetch=10, metrics=[output_rows=1, elapsed_compute=167ns]
   *      FilterExec: _distance@2 IS NOT NULL, metrics=[output_rows=1, elapsed_compute=8.542µs]
   *       SortExec: TopK(fetch=10), expr=[_distance@2 ASC NULLS LAST], metrics=[output_rows=1, elapsed_compute=63.25µs, row_replacements=1]
   *        KNNVectorDistance: metric=l2, metrics=[output_rows=1, elapsed_compute=114.333µs, output_batches=1]
   *         LanceScan: uri=/path/to/data, projection=[vector], row_id=true, row_addr=false, ordered=false, metrics=[output_rows=1, elapsed_compute=103.626µs, bytes_read=549, iops=2, requests=2]
   *
   * @returns A query execution plan with runtime metrics for each step.
   */
  async analyzePlan(): Promise<string> {
    if (this.inner instanceof Promise) {
      return this.inner.then((inner) => inner.analyzePlan());
    } else {
      return this.inner.analyzePlan();
    }
  }
}

/**
 * An interface for a query that can be executed
 *
 * Supported by all query types
 */
export interface ExecutableQuery {}

/**
 * A builder used to construct a vector search
 *
 * This builder can be reused to execute the query many times.
 *
 * @see {@link Query#nearestTo}
 *
 * @hideconstructor
 */
export class VectorQuery extends QueryBase<NativeVectorQuery> {
  /**
   * @hidden
   */
  constructor(inner: NativeVectorQuery | Promise<NativeVectorQuery>) {
    super(inner);
  }

  /**
   * Set the number of partitions to search (probe)
   *
   * This argument is only used when the vector column has an IVF PQ index.
   * If there is no index then this value is ignored.
   *
   * The IVF stage of IVF PQ divides the input into partitions (clusters) of
   * related values.
   *
   * The partition whose centroids are closest to the query vector will be
   * exhaustiely searched to find matches.  This parameter controls how many
   * partitions should be searched.
   *
   * Increasing this value will increase the recall of your query but will
   * also increase the latency of your query.  The default value is 20.  This
   * default is good for many cases but the best value to use will depend on
   * your data and the recall that you need to achieve.
   *
   * For best results we recommend tuning this parameter with a benchmark against
   * your actual data to find the smallest possible value that will still give
   * you the desired recall.
   */
  nprobes(nprobes: number): VectorQuery {
    super.doCall((inner) => inner.nprobes(nprobes));

    return this;
  }

  /*
   * Set the distance range to use
   *
   * Only rows with distances within range [lower_bound, upper_bound)
   * will be returned.
   *
   * `undefined` means no lower or upper bound.
   */
  distanceRange(lowerBound?: number, upperBound?: number): VectorQuery {
    super.doCall((inner) => inner.distanceRange(lowerBound, upperBound));
    return this;
  }

  /**
   * Set the number of candidates to consider during the search
   *
   * This argument is only used when the vector column has an HNSW index.
   * If there is no index then this value is ignored.
   *
   * Increasing this value will increase the recall of your query but will
   * also increase the latency of your query. The default value is 1.5*limit.
   */
  ef(ef: number): VectorQuery {
    super.doCall((inner) => inner.ef(ef));
    return this;
  }

  /**
   * Set the vector column to query
   *
   * This controls which column is compared to the query vector supplied in
   * the call to @see {@link Query#nearestTo}
   *
   * This parameter must be specified if the table has more than one column
   * whose data type is a fixed-size-list of floats.
   */
  column(column: string): VectorQuery {
    super.doCall((inner) => inner.column(column));
    return this;
  }

  /**
   * Set the distance metric to use
   *
   * When performing a vector search we try and find the "nearest" vectors according
   * to some kind of distance metric.  This parameter controls which distance metric to
   * use.  See @see {@link IvfPqOptions.distanceType} for more details on the different
   * distance metrics available.
   *
   * Note: if there is a vector index then the distance type used MUST match the distance
   * type used to train the vector index.  If this is not done then the results will be
   * invalid.
   *
   * By default "l2" is used.
   */
  distanceType(
    distanceType: Required<IvfPqOptions>["distanceType"],
  ): VectorQuery {
    super.doCall((inner) => inner.distanceType(distanceType));
    return this;
  }

  /**
   * A multiplier to control how many additional rows are taken during the refine step
   *
   * This argument is only used when the vector column has an IVF PQ index.
   * If there is no index then this value is ignored.
   *
   * An IVF PQ index stores compressed (quantized) values.  They query vector is compared
   * against these values and, since they are compressed, the comparison is inaccurate.
   *
   * This parameter can be used to refine the results.  It can improve both improve recall
   * and correct the ordering of the nearest results.
   *
   * To refine results LanceDb will first perform an ANN search to find the nearest
   * `limit` * `refine_factor` results.  In other words, if `refine_factor` is 3 and
   * `limit` is the default (10) then the first 30 results will be selected.  LanceDb
   * then fetches the full, uncompressed, values for these 30 results.  The results are
   * then reordered by the true distance and only the nearest 10 are kept.
   *
   * Note: there is a difference between calling this method with a value of 1 and never
   * calling this method at all.  Calling this method with any value will have an impact
   * on your search latency.  When you call this method with a `refine_factor` of 1 then
   * LanceDb still needs to fetch the full, uncompressed, values so that it can potentially
   * reorder the results.
   *
   * Note: if this method is NOT called then the distances returned in the _distance column
   * will be approximate distances based on the comparison of the quantized query vector
   * and the quantized result vectors.  This can be considerably different than the true
   * distance between the query vector and the actual uncompressed vector.
   */
  refineFactor(refineFactor: number): VectorQuery {
    super.doCall((inner) => inner.refineFactor(refineFactor));
    return this;
  }

  /**
   * If this is called then filtering will happen after the vector search instead of
   * before.
   *
   * By default filtering will be performed before the vector search.  This is how
   * filtering is typically understood to work.  This prefilter step does add some
   * additional latency.  Creating a scalar index on the filter column(s) can
   * often improve this latency.  However, sometimes a filter is too complex or scalar
   * indices cannot be applied to the column.  In these cases postfiltering can be
   * used instead of prefiltering to improve latency.
   *
   * Post filtering applies the filter to the results of the vector search.  This means
   * we only run the filter on a much smaller set of data.  However, it can cause the
   * query to return fewer than `limit` results (or even no results) if none of the nearest
   * results match the filter.
   *
   * Post filtering happens during the "refine stage" (described in more detail in
   * @see {@link VectorQuery#refineFactor}).  This means that setting a higher refine
   * factor can often help restore some of the results lost by post filtering.
   */
  postfilter(): VectorQuery {
    super.doCall((inner) => inner.postfilter());
    return this;
  }

  /**
   * If this is called then any vector index is skipped
   *
   * An exhaustive (flat) search will be performed.  The query vector will
   * be compared to every vector in the table.  At high scales this can be
   * expensive.  However, this is often still useful.  For example, skipping
   * the vector index can give you ground truth results which you can use to
   * calculate your recall to select an appropriate value for nprobes.
   */
  bypassVectorIndex(): VectorQuery {
    super.doCall((inner) => inner.bypassVectorIndex());
    return this;
  }

  /*
   * Add a query vector to the search
   *
   * This method can be called multiple times to add multiple query vectors
   * to the search. If multiple query vectors are added, then they will be searched
   * in parallel, and the results will be concatenated. A column called `query_index`
   * will be added to indicate the index of the query vector that produced the result.
   *
   * Performance wise, this is equivalent to running multiple queries concurrently.
   */
  addQueryVector(vector: IntoVector): VectorQuery {
    if (vector instanceof Promise) {
      const res = (async () => {
        try {
          const v = await vector;
          const arr = Float32Array.from(v);
          //
          // biome-ignore lint/suspicious/noExplicitAny: we need to get the `inner`, but js has no package scoping
          const value: any = this.addQueryVector(arr);
          const inner = value.inner as
            | NativeVectorQuery
            | Promise<NativeVectorQuery>;
          return inner;
        } catch (e) {
          return Promise.reject(e);
        }
      })();
      return new VectorQuery(res);
    } else {
      super.doCall((inner) => {
        inner.addQueryVector(Float32Array.from(vector));
      });
      return this;
    }
  }

  rerank(reranker: Reranker): VectorQuery {
    super.doCall((inner) =>
      inner.rerank({
        rerankHybrid: async (_, args) => {
          const vecResults = await fromBufferToRecordBatch(args.vecResults);
          const ftsResults = await fromBufferToRecordBatch(args.ftsResults);
          const result = await reranker.rerankHybrid(
            args.query,
            vecResults as RecordBatch,
            ftsResults as RecordBatch,
          );

          const buffer = fromRecordBatchToBuffer(result);
          return buffer;
        },
      }),
    );

    return this;
  }
}

/** A builder for LanceDB queries.
 *
 * @see {@link Table#query}, {@link Table#search}
 *
 * @hideconstructor
 */
export class Query extends QueryBase<NativeQuery> {
  /**
   * @hidden
   */
  constructor(tbl: NativeTable) {
    super(tbl.query());
  }

  /**
   * Find the nearest vectors to the given query vector.
   *
   * This converts the query from a plain query to a vector query.
   *
   * This method will attempt to convert the input to the query vector
   * expected by the embedding model.  If the input cannot be converted
   * then an error will be thrown.
   *
   * By default, there is no embedding model, and the input should be
   * an array-like object of numbers (something that can be used as input
   * to Float32Array.from)
   *
   * If there is only one vector column (a column whose data type is a
   * fixed size list of floats) then the column does not need to be specified.
   * If there is more than one vector column you must use
   * @see {@link VectorQuery#column}  to specify which column you would like
   * to compare with.
   *
   * If no index has been created on the vector column then a vector query
   * will perform a distance comparison between the query vector and every
   * vector in the database and then sort the results.  This is sometimes
   * called a "flat search"
   *
   * For small databases, with a few hundred thousand vectors or less, this can
   * be reasonably fast.  In larger databases you should create a vector index
   * on the column.  If there is a vector index then an "approximate" nearest
   * neighbor search (frequently called an ANN search) will be performed.  This
   * search is much faster, but the results will be approximate.
   *
   * The query can be further parameterized using the returned builder.  There
   * are various ANN search parameters that will let you fine tune your recall
   * accuracy vs search latency.
   *
   * Vector searches always have a `limit`.  If `limit` has not been called then
   * a default `limit` of 10 will be used.  @see {@link Query#limit}
   */
  nearestTo(vector: IntoVector): VectorQuery {
    if (this.inner instanceof Promise) {
      const nativeQuery = this.inner.then(async (inner) => {
        if (vector instanceof Promise) {
          const arr = await vector.then((v) => Float32Array.from(v));
          return inner.nearestTo(arr);
        } else {
          return inner.nearestTo(Float32Array.from(vector));
        }
      });
      return new VectorQuery(nativeQuery);
    }
    if (vector instanceof Promise) {
      const res = (async () => {
        try {
          const v = await vector;
          const arr = Float32Array.from(v);
          //
          // biome-ignore lint/suspicious/noExplicitAny: we need to get the `inner`, but js has no package scoping
          const value: any = this.nearestTo(arr);
          const inner = value.inner as
            | NativeVectorQuery
            | Promise<NativeVectorQuery>;
          return inner;
        } catch (e) {
          return Promise.reject(e);
        }
      })();
      return new VectorQuery(res);
    } else {
      const vectorQuery = this.inner.nearestTo(Float32Array.from(vector));
      return new VectorQuery(vectorQuery);
    }
  }

  nearestToText(query: string | FullTextQuery, columns?: string[]): Query {
    this.doCall((inner) => {
      if (typeof query === "string") {
        inner.fullTextSearch({
          query: query,
          columns: columns,
        });
      } else {
        const queryObj = query.toDict();
        inner.fullTextSearch(queryObj);
      }
    });
    return this;
  }
}

export enum FullTextQueryType {
  Match = "match",
  MatchPhrase = "match_phrase",
  Boost = "boost",
  MultiMatch = "multi_match",
}

export interface FullTextQuery {
  queryType(): FullTextQueryType;
  toDict(): Record<string, unknown>;
}

export class MatchQuery implements FullTextQuery {
  constructor(
    private query: string,
    private column: string,
    private boost: number = 1.0,
    private fuzziness: number = 0,
    private maxExpansions: number = 50,
  ) {}

  queryType(): FullTextQueryType {
    return FullTextQueryType.Match;
  }

  toDict(): Record<string, unknown> {
    return {
      [this.queryType()]: {
        [this.column]: {
          query: this.query,
          boost: this.boost,
          fuzziness: this.fuzziness,
          // biome-ignore lint/style/useNamingConvention: use underscore for consistency with the other APIs
          max_expansions: this.maxExpansions,
        },
      },
    };
  }
}

export class PhraseQuery implements FullTextQuery {
  constructor(
    private query: string,
    private column: string,
  ) {}

  queryType(): FullTextQueryType {
    return FullTextQueryType.MatchPhrase;
  }

  toDict(): Record<string, unknown> {
    return {
      [this.queryType()]: {
        [this.column]: this.query,
      },
    };
  }
}

export class BoostQuery implements FullTextQuery {
  constructor(
    private positive: FullTextQuery,
    private negative: FullTextQuery,
    private negativeBoost: number,
  ) {}

  queryType(): FullTextQueryType {
    return FullTextQueryType.Boost;
  }

  toDict(): Record<string, unknown> {
    return {
      [this.queryType()]: {
        positive: this.positive.toDict(),
        negative: this.negative.toDict(),
        // biome-ignore lint/style/useNamingConvention: use underscore for consistency with the other APIs
        negative_boost: this.negativeBoost,
      },
    };
  }
}

export class MultiMatchQuery implements FullTextQuery {
  constructor(
    private query: string,
    private columns: string[],
    private boosts: number[] = columns.map(() => 1.0),
  ) {}

  queryType(): FullTextQueryType {
    return FullTextQueryType.MultiMatch;
  }

  toDict(): Record<string, unknown> {
    return {
      [this.queryType()]: {
        query: this.query,
        columns: this.columns,
        boost: this.boosts,
      },
    };
  }
}
