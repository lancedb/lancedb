// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors
import { Data, Schema, fromDataToBuffer } from "./arrow";
import { MergeResult, NativeMergeInsertBuilder } from "./native";

/** A builder used to create and run a merge insert operation */
export class MergeInsertBuilder {
  #native: NativeMergeInsertBuilder;
  #schema: Schema | Promise<Schema>;

  /** Construct a MergeInsertBuilder. __Internal use only.__ */
  constructor(
    native: NativeMergeInsertBuilder,
    schema: Schema | Promise<Schema>,
  ) {
    this.#native = native;
    this.#schema = schema;
  }

  /**
   * Rows that exist in both the source table (new data) and
   * the target table (old data) will be updated, replacing
   * the old row with the corresponding matching row.
   *
   * If there are multiple matches then the behavior is undefined.
   * Currently this causes multiple copies of the row to be created
   * but that behavior is subject to change.
   *
   * An optional condition may be specified.  If it is, then only
   * matched rows that satisfy the condition will be updated.  Any
   * rows that do not satisfy the condition will be left as they
   * are.  Failing to satisfy the condition does not cause a
   * "matched row" to become a "not matched" row.
   *
   * The condition should be an SQL string.  Use the prefix
   * target. to refer to rows in the target table (old data)
   * and the prefix source. to refer to rows in the source
   * table (new data).
   *
   * For example, "target.last_update < source.last_update"
   */
  whenMatchedUpdateAll(options?: { where: string }): MergeInsertBuilder {
    return new MergeInsertBuilder(
      this.#native.whenMatchedUpdateAll(options?.where),
      this.#schema,
    );
  }
  /**
   * Rows that exist only in the source table (new data) should
   * be inserted into the target table.
   */
  whenNotMatchedInsertAll(): MergeInsertBuilder {
    return new MergeInsertBuilder(
      this.#native.whenNotMatchedInsertAll(),
      this.#schema,
    );
  }
  /**
   * Rows that exist only in the target table (old data) will be
   * deleted.  An optional condition can be provided to limit what
   * data is deleted.
   *
   * @param options.where - An optional condition to limit what data is deleted
   */
  whenNotMatchedBySourceDelete(options?: {
    where: string;
  }): MergeInsertBuilder {
    return new MergeInsertBuilder(
      this.#native.whenNotMatchedBySourceDelete(options?.where),
      this.#schema,
    );
  }

  /**
   * Controls whether to use indexes for the merge operation.
   *
   * On the standard (non-MemWAL) merge path, `true` (the default) allows an
   * indexed probe when every join key has a usable scalar index. `false`
   * forces a full-table join even when those indexes exist.
   *
   * The indexed path also scans fragments not covered by every join-key
   * index and combines those rows with index matches. Appends and updates
   * that remove join-key index coverage can therefore add scan cost to
   * later merges, even when index use is enabled.
   *
   * For a partial-column update, the indexed path patches the supplied
   * columns for every row in each fragment with a match. Scalar indexes on
   * patched columns lose coverage for those fragments. The full-table path
   * instead rewrites matched rows as complete rows, but its join can
   * materialize the entire target table in memory. Do not use `false` as a
   * general optimization for sparse updates on large tables. See the
   * [merge execution guide](https://docs.lancedb.com/tables/update/#how-merge-insert-executes).
   *
   * @param useIndex - Whether to allow an indexed probe. Defaults to `true`.
   */
  useIndex(useIndex: boolean): MergeInsertBuilder {
    return new MergeInsertBuilder(
      this.#native.useIndex(useIndex),
      this.#schema,
    );
  }
  /**
   * Control MemWAL routing for this merge.
   *
   * By default (unset), a `mergeInsert` on a table with an LSM write spec is
   * routed through Lance's MemWAL shard writer, and a table without one uses the
   * standard path.
   *
   * @param enable - `true` forces MemWAL routing and errors if the table has no
   * LSM write spec. `false` forces the standard write path even when a spec is set.
   */
  useLsm(enable: boolean): MergeInsertBuilder {
    return new MergeInsertBuilder(this.#native.useLsm(enable), this.#schema);
  }
  /**
   * Controls how an LSM merge checks that its input targets a single shard.
   *
   * When a table has an LSM write spec, every row in a `mergeInsert` call must
   * route to the same shard. When `true` (the default), every row is inspected
   * to verify this. When `false`, only the first row is inspected and the
   * shard it routes to is used for the whole input — a faster path for callers
   * that have already pre-sharded their input. Has no effect on tables without
   * an LSM write spec.
   *
   * @param validateSingleShard - Whether to check every row routes to one shard. Defaults to `true`.
   */
  validateSingleShard(validateSingleShard: boolean): MergeInsertBuilder {
    return new MergeInsertBuilder(
      this.#native.validateSingleShard(validateSingleShard),
      this.#schema,
    );
  }
  /**
   * Executes the merge insert operation
   *
   * @returns {Promise<MergeResult>} the merge result
   */
  async execute(
    data: Data,
    execOptions?: Partial<WriteExecutionOptions>,
  ): Promise<MergeResult> {
    let schema: Schema;
    if (this.#schema instanceof Promise) {
      schema = await this.#schema;
      this.#schema = schema; // In case of future calls
    } else {
      schema = this.#schema;
    }

    if (execOptions?.timeoutMs !== undefined) {
      this.#native.setTimeout(execOptions.timeoutMs);
    }

    const buffer = await fromDataToBuffer(data, undefined, schema);
    return await this.#native.execute(buffer);
  }
}

export interface WriteExecutionOptions {
  /**
   * Maximum time to run the operation before cancelling it.
   *
   * By default, there is a 30-second timeout that is only enforced after the
   * first attempt. This is to prevent spending too long retrying to resolve
   * conflicts. For example, if a write attempt takes 20 seconds and fails,
   * the second attempt will be cancelled after 10 seconds, hitting the
   * 30-second timeout. However, a write that takes one hour and succeeds on the
   * first attempt will not be cancelled.
   *
   * When this is set, the timeout is enforced on all attempts, including the first.
   */
  timeoutMs?: number;
}
