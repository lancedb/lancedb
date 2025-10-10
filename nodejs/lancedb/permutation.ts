// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import {
  PermutationBuilder as NativePermutationBuilder,
  Table as NativeTable,
  ShuffleOptions,
  SplitHashOptions,
  SplitRandomOptions,
  SplitSequentialOptions,
  permutationBuilder as nativePermutationBuilder,
} from "./native.js";
import { LocalTable, Table } from "./table";

/**
 * A PermutationBuilder for creating data permutations with splits, shuffling, and filtering.
 *
 * This class provides a TypeScript wrapper around the native Rust PermutationBuilder,
 * offering methods to configure data splits, shuffling, and filtering before executing
 * the permutation to create a new table.
 */
export class PermutationBuilder {
  private inner: NativePermutationBuilder;

  /**
   * @hidden
   */
  constructor(inner: NativePermutationBuilder) {
    this.inner = inner;
  }

  /**
   * Configure random splits for the permutation.
   *
   * @param options - Configuration for random splitting
   * @returns A new PermutationBuilder instance
   * @example
   * ```ts
   * // Split by ratios
   * builder.splitRandom({ ratios: [0.7, 0.3], seed: 42 });
   *
   * // Split by counts
   * builder.splitRandom({ counts: [1000, 500], seed: 42 });
   *
   * // Split with fixed size
   * builder.splitRandom({ fixed: 100, seed: 42 });
   * ```
   */
  splitRandom(options: SplitRandomOptions): PermutationBuilder {
    const newInner = this.inner.splitRandom(options);
    return new PermutationBuilder(newInner);
  }

  /**
   * Configure hash-based splits for the permutation.
   *
   * @param options - Configuration for hash-based splitting
   * @returns A new PermutationBuilder instance
   * @example
   * ```ts
   * builder.splitHash({
   *   columns: ["user_id"],
   *   splitWeights: [70, 30],
   *   discardWeight: 0
   * });
   * ```
   */
  splitHash(options: SplitHashOptions): PermutationBuilder {
    const newInner = this.inner.splitHash(options);
    return new PermutationBuilder(newInner);
  }

  /**
   * Configure sequential splits for the permutation.
   *
   * @param options - Configuration for sequential splitting
   * @returns A new PermutationBuilder instance
   * @example
   * ```ts
   * // Split by ratios
   * builder.splitSequential({ ratios: [0.8, 0.2] });
   *
   * // Split by counts
   * builder.splitSequential({ counts: [800, 200] });
   *
   * // Split with fixed size
   * builder.splitSequential({ fixed: 1000 });
   * ```
   */
  splitSequential(options: SplitSequentialOptions): PermutationBuilder {
    const newInner = this.inner.splitSequential(options);
    return new PermutationBuilder(newInner);
  }

  /**
   * Configure calculated splits for the permutation.
   *
   * @param calculation - SQL expression for calculating splits
   * @returns A new PermutationBuilder instance
   * @example
   * ```ts
   * builder.splitCalculated("user_id % 3");
   * ```
   */
  splitCalculated(calculation: string): PermutationBuilder {
    const newInner = this.inner.splitCalculated(calculation);
    return new PermutationBuilder(newInner);
  }

  /**
   * Configure shuffling for the permutation.
   *
   * @param options - Configuration for shuffling
   * @returns A new PermutationBuilder instance
   * @example
   * ```ts
   * // Basic shuffle
   * builder.shuffle({ seed: 42 });
   *
   * // Shuffle with clump size
   * builder.shuffle({ seed: 42, clumpSize: 10 });
   * ```
   */
  shuffle(options: ShuffleOptions): PermutationBuilder {
    const newInner = this.inner.shuffle(options);
    return new PermutationBuilder(newInner);
  }

  /**
   * Configure filtering for the permutation.
   *
   * @param filter - SQL filter expression
   * @returns A new PermutationBuilder instance
   * @example
   * ```ts
   * builder.filter("age > 18 AND status = 'active'");
   * ```
   */
  filter(filter: string): PermutationBuilder {
    const newInner = this.inner.filter(filter);
    return new PermutationBuilder(newInner);
  }

  /**
   * Execute the permutation and create the destination table.
   *
   * @returns A Promise that resolves to the new Table instance
   * @example
   * ```ts
   * const permutationTable = await builder.execute();
   * console.log(`Created table: ${permutationTable.name}`);
   * ```
   */
  async execute(): Promise<Table> {
    const nativeTable: NativeTable = await this.inner.execute();
    return new LocalTable(nativeTable);
  }
}

/**
 * Create a permutation builder for the given table.
 *
 * @param table - The source table to create a permutation from
 * @returns A PermutationBuilder instance
 * @example
 * ```ts
 * const builder = permutationBuilder(sourceTable, "training_data")
 *   .splitRandom({ ratios: [0.8, 0.2], seed: 42 })
 *   .shuffle({ seed: 123 });
 *
 * const trainingTable = await builder.execute();
 * ```
 */
export function permutationBuilder(
  table: Table,
): PermutationBuilder {
  // Extract the inner native table from the TypeScript wrapper
  const localTable = table as LocalTable;
  // Access inner through type assertion since it's private
  const nativeBuilder = nativePermutationBuilder(
    // biome-ignore lint/suspicious/noExplicitAny: need access to private variable
    (localTable as any).inner,
  );
  return new PermutationBuilder(nativeBuilder);
}
