// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

/** Actual committed outcome of one remote compaction job. */
export interface CompactionResult {
  /** A bounded run left eligible work for another invocation. */
  moreWork: boolean;
  /** Some planned groups were not committed. */
  partialCommit: boolean;
  /** Live source rows in committed rewrite groups. */
  rowsSucceeded: number;
  /** Planned live rows not committed by this job. */
  rowsFailed: number;
  /** Percentage of planned live rows committed, from 0 to 100. */
  completionPercentage: number;
  /** Data files added by committed rewrites; null if an older worker omitted it. */
  filesAdded: number | null;
  /** Data files removed by committed rewrites; null if an older worker omitted it. */
  filesRemoved: number | null;
  /** Fragments added by committed rewrites; null if an older worker omitted it. */
  fragmentsAdded: number | null;
  /** Fragments removed by committed rewrites; null if an older worker omitted it. */
  fragmentsRemoved: number | null;
}

/** @internal */
export function decodeCompactionResult(value: unknown): CompactionResult {
  // The Rust typed-job decoder validates the wire payload before it reaches JS.
  const result = value as Record<string, number | boolean | null>;
  return {
    moreWork: result.more_work as boolean,
    partialCommit: result.partial_commit as boolean,
    rowsSucceeded: result.rows_succeeded as number,
    rowsFailed: result.rows_failed as number,
    completionPercentage: result.completion_percentage as number,
    filesAdded: (result.files_added as number | null) ?? null,
    filesRemoved: (result.files_removed as number | null) ?? null,
    fragmentsAdded: (result.fragments_added as number | null) ?? null,
    fragmentsRemoved: (result.fragments_removed as number | null) ?? null,
  };
}
