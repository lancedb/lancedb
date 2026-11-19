// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

/** Actual committed outcome of one Enterprise Optimize data rewrite. */
export interface OptimizeResult {
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
  /** Files added, including deletion files; null if an older worker omitted it. */
  filesAdded: number | null;
  /** Files removed, including deletion files; null if an older worker omitted it. */
  filesRemoved: number | null;
  /** Fragments added by committed rewrites; null if an older worker omitted it. */
  fragmentsAdded: number | null;
  /** Fragments removed by committed rewrites; null if an older worker omitted it. */
  fragmentsRemoved: number | null;
}

/** @internal */
export function decodeOptimizeResult(value: unknown): OptimizeResult {
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

/** Effective settings after Enterprise policy resolution. */
export interface OptimizeResolvedOptions {
  targetRowsPerFragment: number;
  maxSourceRows: number | null;
  /** Zero disables byte-based I/O backpressure; null uses the worker default. */
  ioBufferSize: number | null;
  batchSize: number | null;
}

/** Data-rewrite preview without creating a maintenance job.
 * Counts describe planned work, not committed results, runtime, or memory use.
 * A later submission chooses its own snapshot and may select different work. */
export interface OptimizePlan {
  operation: "rewrite_data";
  table: string;
  namespace: string[];
  branch: string;
  sourceVersion: number;
  effectiveOptions: OptimizeResolvedOptions;
  plannedGroups: number;
  plannedFragments: number;
  plannedSourceRows: number;
  moreWork: boolean;
}

/** @internal */
export function decodeOptimizePlan(value: unknown): OptimizePlan {
  // Rust validates the versioned SQL response before exposing it to JS.
  const plan = value as Record<string, unknown>;
  const options = plan.effective_options as Record<string, number | null>;
  return {
    operation: plan.operation as "rewrite_data",
    table: plan.table as string,
    namespace: plan.namespace as string[],
    branch: plan.branch as string,
    sourceVersion: plan.source_version as number,
    plannedGroups: plan.planned_groups as number,
    plannedFragments: plan.planned_fragments as number,
    plannedSourceRows: plan.planned_source_rows as number,
    moreWork: plan.more_work as boolean,
    effectiveOptions: {
      targetRowsPerFragment: options.target_rows_per_fragment as number,
      maxSourceRows: options.max_source_rows ?? null,
      ioBufferSize: options.io_buffer_size ?? null,
      batchSize: options.batch_size ?? null,
    },
  };
}
