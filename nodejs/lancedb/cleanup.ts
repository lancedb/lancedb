// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

/** Per-run retention; omission inherits selected table/branch then deployment policy. */
export interface CleanupOptions {
  /** Minimum old-version age in seconds. Zero is explicit, not omitted. */
  minAgeSeconds?: number;
}

/** Version-deletion outcome. Best-effort auxiliary sweeps are excluded. */
export interface CleanupResult {
  formatVersion: number;
  minAgeSeconds: number;
  /** False after uncertain retry/recovery; both counts are then null. */
  statisticsComplete: boolean;
  oldVersions: number | null;
  bytesRemoved: number | null;
  /** Failed object deletions, if measured. Successful removals are retained. */
  failedDeletes: number | null;
}

/** Read-only deletion estimates; no job or deletion snapshot is reserved. */
export interface CleanupPlan {
  formatVersion: number;
  sourceVersion: number;
  minAgeSeconds: number;
  cutoffMillis: number;
  versionsToRemove: number;
  bytesToRemove: number;
}

/** @internal */
export function decodeCleanupPlan(value: unknown): CleanupPlan {
  if (typeof value !== "object" || value === null)
    throw new Error("invalid cleanup preview");
  const plan = value as Record<string, unknown>;
  if (
    plan.format_version !== 1 ||
    [
      "source_version",
      "min_age_seconds",
      "cutoff_millis",
      "versions_to_remove",
      "bytes_to_remove",
    ].some(
      (key) =>
        !Number.isSafeInteger(plan[key]) ||
        (key !== "cutoff_millis" && (plan[key] as number) < 0),
    )
  ) {
    throw new Error("unsupported or invalid cleanup preview");
  }
  return {
    formatVersion: 1,
    sourceVersion: plan.source_version as number,
    minAgeSeconds: plan.min_age_seconds as number,
    cutoffMillis: plan.cutoff_millis as number,
    versionsToRemove: plan.versions_to_remove as number,
    bytesToRemove: plan.bytes_to_remove as number,
  };
}

/** @internal */
export function validateCleanupOptions(options?: CleanupOptions): void {
  for (const key of Object.keys(options ?? {})) {
    if (key !== "minAgeSeconds")
      throw new Error(`Unknown cleanup option: ${key}`);
  }
  const age = options?.minAgeSeconds;
  if (age !== undefined && (!Number.isSafeInteger(age) || age < 0)) {
    throw new Error("minAgeSeconds must be a nonnegative safe integer");
  }
}

/** @internal */
export function decodeCleanupResult(value: unknown): CleanupResult {
  if (typeof value !== "object" || value === null)
    throw new Error("invalid cleanup result");
  const result = value as Record<string, unknown>;
  const counts = [result.old_versions ?? null, result.bytes_removed ?? null];
  const failed = result.failed_deletes ?? null;
  if (
    failed !== null &&
    (!Number.isSafeInteger(failed) ||
      (failed as number) < 0 ||
      result.statistics_complete !== true)
  ) {
    throw new Error("invalid cleanup failed-deletion measurement");
  }
  if (
    result.format_version !== 1 ||
    typeof result.statistics_complete !== "boolean" ||
    !Number.isSafeInteger(result.min_age_seconds) ||
    (result.min_age_seconds as number) < 0 ||
    counts.some(
      (count) =>
        count !== null &&
        (!Number.isSafeInteger(count) || (count as number) < 0),
    ) ||
    (result.statistics_complete
      ? counts.some((count) => count === null)
      : counts.some((count) => count !== null))
  ) {
    throw new Error("unsupported or inconsistent cleanup result");
  }
  return {
    formatVersion: 1,
    minAgeSeconds: result.min_age_seconds as number,
    statisticsComplete: result.statistics_complete,
    oldVersions: counts[0] as number | null,
    bytesRemoved: counts[1] as number | null,
    failedDeletes: (result.failed_deletes as number | null) ?? null,
  };
}
