// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

/** Reported executor work for a pinned snapshot; unavailable measurements stay null. */
export interface PrewarmDataResult {
  /** The source snapshot captured by submission. */
  tableVersion: number;
  /** Reported executors targeted, or null when unavailable. */
  executorsTargeted: number | null;
  /** Executors that finished successfully or partially, or null when unavailable. */
  executorsCompleted: number | null;
  /** Subset of completed executors reporting partial warming, or null when unavailable. */
  executorsPartial: number | null;
  /** Reported fragments selected, or null when unavailable. */
  fragmentsSelected: number | null;
  /** Reported ranges completed, or null when unavailable. */
  rangesCompleted: number | null;
  /** Reported bytes read, or null when unavailable. */
  bytesRead: number | null;
}

/** @internal */
export function decodePrewarmDataResult(value: unknown): PrewarmDataResult {
  // Rust validates the typed wire result before handing it to JavaScript.
  const result = value as Record<string, number | null>;
  return {
    tableVersion: result.table_version as number,
    executorsTargeted: result.executors_targeted ?? null,
    executorsCompleted: result.executors_completed ?? null,
    executorsPartial: result.executors_partial ?? null,
    fragmentsSelected: result.fragments_selected ?? null,
    rangesCompleted: result.ranges_completed ?? null,
    bytesRead: result.bytes_read ?? null,
  };
}

/** Metadata-only snapshot preview; no warming job is submitted. */
export interface PrewarmDataPlan {
  /** Snapshot used for this preview. A later submission plans its own snapshot. */
  tableVersion: number;
  /** Readable display text. Use pruning for structured facts. */
  plan: string;
  pruning: PrewarmPruningSummary;
}

/** Conservative metadata proof, rather than exact matching rows. */
export interface PrewarmPruningSummary {
  /** Server-reported proof mode; future servers may add modes. */
  mode: string;
  inputFragments: number;
  selectedFragments: number;
  prunedFragments: number;
  /** Candidate physical intervals, or null when unknown. */
  selectedRowRanges: number | null;
  fallbackReason: string | null;
}

/** @internal */
export function decodePrewarmDataPlan(value: unknown): PrewarmDataPlan {
  // Rust validates the versioned response before handing it to JavaScript.
  const result = value as {
    [key: string]: unknown;
    pruning: Record<string, number | string | null>;
  };
  return {
    tableVersion: result.table_version as number,
    plan: result.plan as string,
    pruning: {
      mode: result.pruning.mode as string,
      inputFragments: result.pruning.input_fragments as number,
      selectedFragments: result.pruning.selected_fragments as number,
      prunedFragments: result.pruning.pruned_fragments as number,
      selectedRowRanges:
        (result.pruning.selected_row_ranges as number | null) ?? null,
      fallbackReason: (result.pruning.fallback_reason as string | null) ?? null,
    },
  };
}
