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
