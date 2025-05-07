// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors
import { exec } from "child_process";
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
   * matched rows that satisfy the condtion will be updated.  Any
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
   * Maximum time (in milliseconds) to run the operation before cancelling it.
   * Default is no timeout for first attempt, but an overall timeout of
   * 30 seconds is applied after that.
   */
  timeoutMs?: number;
}
