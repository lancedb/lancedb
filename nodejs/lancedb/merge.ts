import { Data, fromDataToBuffer } from "./arrow";
import { NativeMergeInsertBuilder } from "./native";

/** A builder used to create and run a merge insert operation */
export class MergeInsertBuilder {
  #native: NativeMergeInsertBuilder;

  /** Construct a MergeInsertBuilder. __Internal use only.__ */
  constructor(native: NativeMergeInsertBuilder) {
    this.#native = native;
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
    );
  }
  /**
   * Rows that exist only in the source table (new data) should
   * be inserted into the target table.
   */
  whenNotMatchedInsertAll(): MergeInsertBuilder {
    return new MergeInsertBuilder(this.#native.whenNotMatchedInsertAll());
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
    );
  }
  /**
   * Executes the merge insert operation
   *
   * Nothing is returned but the `Table` is updated
   */
  async execute(data: Data): Promise<void> {
    const buffer = await fromDataToBuffer(data);
    await this.#native.execute(buffer);
  }
}
