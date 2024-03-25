// Copyright 2024 Lance Developers.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import { RecordBatch, tableFromIPC, Table as ArrowTable } from "apache-arrow";
import {
  RecordBatchIterator as NativeBatchIterator,
  Query as NativeQuery,
  Table as NativeTable,
  VectorQuery as NativeVectorQuery,
} from "./native";
import { type IvfPqOptions } from "./indices";
export class RecordBatchIterator implements AsyncIterator<RecordBatch> {
  private promisedInner?: Promise<NativeBatchIterator>;
  private inner?: NativeBatchIterator;

  constructor(promise?: Promise<NativeBatchIterator>) {
    // TODO: check promise reliably so we dont need to pass two arguments.
    this.promisedInner = promise;
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
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

/** Common methods supported by all query types */
export class QueryBase<
  NativeQueryType extends NativeQuery | NativeVectorQuery,
  QueryType,
> implements AsyncIterable<RecordBatch>
{
  protected constructor(protected inner: NativeQueryType) {}

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
  where(predicate: string): QueryType {
    this.inner.onlyIf(predicate);
    return this as unknown as QueryType;
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
    columns: string[] | Map<string, string> | Record<string, string>,
  ): QueryType {
    let columnTuples: [string, string][];
    if (Array.isArray(columns)) {
      columnTuples = columns.map((c) => [c, c]);
    } else if (columns instanceof Map) {
      columnTuples = Array.from(columns.entries());
    } else {
      columnTuples = Object.entries(columns);
    }
    this.inner.select(columnTuples);
    return this as unknown as QueryType;
  }

  /**
   * Set the maximum number of results to return.
   *
   * By default, a plain search has no limit.  If this method is not
   * called then every valid row from the table will be returned.
   */
  limit(limit: number): QueryType {
    this.inner.limit(limit);
    return this as unknown as QueryType;
  }

  protected nativeExecute(): Promise<NativeBatchIterator> {
    return this.inner.execute();
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
  protected execute(): RecordBatchIterator {
    return new RecordBatchIterator(this.nativeExecute());
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  [Symbol.asyncIterator](): AsyncIterator<RecordBatch<any>> {
    const promise = this.nativeExecute();
    return new RecordBatchIterator(promise);
  }

  /** Collect the results as an Arrow @see {@link ArrowTable}. */
  async toArrow(): Promise<ArrowTable> {
    const batches = [];
    for await (const batch of this) {
      batches.push(batch);
    }
    return new ArrowTable(batches);
  }

  /** Collect the results as an array of objects. */
  async toArray(): Promise<unknown[]> {
    const tbl = await this.toArrow();
    // eslint-disable-next-line @typescript-eslint/no-unsafe-return
    return tbl.toArray();
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
 */
export class VectorQuery extends QueryBase<NativeVectorQuery, VectorQuery> {
  constructor(inner: NativeVectorQuery) {
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
    this.inner.nprobes(nprobes);
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
    this.inner.column(column);
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
  distanceType(distanceType: string): VectorQuery {
    this.inner.distanceType(distanceType);
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
    this.inner.refineFactor(refineFactor);
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
    this.inner.postfilter();
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
    this.inner.bypassVectorIndex();
    return this;
  }
}

/** A builder for LanceDB queries. */
export class Query extends QueryBase<NativeQuery, Query> {
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
  nearestTo(vector: unknown): VectorQuery {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const vectorQuery = this.inner.nearestTo(Float32Array.from(vector as any));
    return new VectorQuery(vectorQuery);
  }
}
