// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import type {
  FunctionErrorGroup,
  FunctionErrorRecord,
  FunctionErrors,
  RefreshColumnOptions,
} from "./native";
import type { Table } from "./table";

/** Options for retrying exactly the current failures of one Function column. */
export interface RetryFailedOptions
  extends Omit<RefreshColumnOptions, "where"> {
  /** Only retry failures with this exception class. */
  errorType?: string;
  /** Only retry current failures recorded by this job. */
  jobId?: string;
  /** Explicitly release and retry matching quarantined failures. */
  includeQuarantined?: boolean;
  /** Upgrade to this immutable Function version while retrying failures. */
  functionVersion?: string;
}

/**
 * Which existing values to recompute during a Function version upgrade.
 * `"failed"` selects current failures. `"all"` refreshes every eligible live
 * row. `"none"` preserves existing values and only changes the binding.
 */
export type FunctionRecompute = "failed" | "all" | "none";

/** Options for assigning a new immutable Function version to a column. */
export interface UpgradeColumnOptions
  extends Omit<RefreshColumnOptions, "where"> {
  /** An existing immutable version of the bound Function. */
  version: string;
  /** Defaults to `"all"`. Quarantined failures are excluded. */
  recompute?: FunctionRecompute;
}

/** The terminal outcome of a Function retry or upgrade job. */
export interface RefreshColumnJobResult {
  /** Logical rows successfully assigned a value. */
  rowsAssigned: bigint;
  /** Logical rows whose computation failed. */
  rowsFailed: bigint;
  /** Logical rows still needing a value, excluding rows counted in `rowsFailed`. */
  rowsRemaining: bigint;
  /** The exact table version read by the job. */
  sourceVersion: bigint;
  /** The table version made visible by the job, if it published one. */
  publishedVersion?: bigint;
  /** Grouped causes reported by the completed job. */
  errorGroups: FunctionErrorGroup[];
  /** Whether the reported cause counts omit any failures. */
  errorGroupsTruncated: boolean;
}

/** @internal */
export function decodeRefreshColumnJobResult(
  value: unknown,
): RefreshColumnJobResult {
  // The Rust typed job validates the terminal result and preserves u64 values.
  const result = value as {
    ["rows_assigned"]: number | bigint;
    ["rows_failed"]: number | bigint;
    ["rows_remaining"]: number | bigint;
    ["source_version"]: number | bigint;
    ["published_version"]?: number | bigint;
    ["error_groups"]?: {
      column: string;
      ["error_type"]: string;
      ["error_message"]: string;
      ["rows_failed"]: number | bigint;
    }[];
    ["error_groups_truncated"]?: boolean;
  };
  return {
    rowsAssigned: BigInt(result.rows_assigned),
    rowsFailed: BigInt(result.rows_failed),
    rowsRemaining: BigInt(result.rows_remaining),
    sourceVersion: BigInt(result.source_version),
    publishedVersion:
      result.published_version === undefined
        ? undefined
        : BigInt(result.published_version),
    errorGroups: (result.error_groups ?? []).map((group) => ({
      column: group.column,
      errorType: group.error_type,
      errorMessage: group.error_message,
      rowsFailed: BigInt(group.rows_failed),
    })),
    errorGroupsTruncated: result.error_groups_truncated ?? false,
  };
}

/** A failed Function-column row and its source data at the table's read snapshot. */
export interface FunctionFailedRow {
  /** Values of the requested source columns. */
  input: Record<string, unknown>;
  /** The failure for this row and Function column. */
  error: FunctionErrorRecord;
}

/** A Function error listing with grouped causes and current source-row lookup. */
export interface FunctionErrorListing extends FunctionErrors {
  /** Group by column, exception class and message. Requires a complete workflow-v1 listing. */
  summary(): FunctionErrorGroup[];
  /**
   * Read current failing inputs, one result per error record. Defaults to all
   * source columns. Requires a complete current listing with row identities.
   * Throws if a failed row is no longer addressable in the table's read snapshot.
   */
  rows(columns?: string[]): Promise<FunctionFailedRow[]>;
}

/** @internal */
export function functionErrorListing(
  table: Table,
  errors: FunctionErrors,
  historical: boolean,
): FunctionErrorListing {
  const listing: FunctionErrorListing = {
    ...errors,
    summary() {
      requireCompleteListing(errors);
      return errors.errorGroups ?? [];
    },
    async rows(columns) {
      requireCompleteListing(errors);
      if (historical) {
        throw new Error("Source-row lookup requires current Function failures");
      }
      return readFailedRows(table, errors.records, columns);
    },
  };
  // Keep the enumerable listing fields compatible with existing callers.
  return Object.defineProperties(listing, {
    summary: { enumerable: false },
    rows: { enumerable: false },
  });
}

function requireCompleteListing(errors: FunctionErrors): void {
  if ((errors.workflowVersion ?? 0) < 1) {
    throw new Error(
      "The server does not support the Function row-failure workflow",
    );
  }
  if (
    errors.truncated ||
    errors.fragments.length > 0 ||
    errors.records.some(
      (record) => record.rowOffset == null && record.input == null,
    )
  ) {
    throw new Error(
      "Function errors are incomplete; narrow the request or raise limit",
    );
  }
}

async function readFailedRows(
  table: Table,
  records: FunctionErrorRecord[],
  columns?: string[],
): Promise<FunctionFailedRow[]> {
  if (records.length === 0) return [];

  const stableIds = new Set<bigint>();
  const addresses = new Set<bigint>();
  const keys = records.map((record) => {
    if (record.input != null) {
      throw new Error(
        "Inspect the input source identities for a materialized-view Function error",
      );
    }
    if (typeof record.rowId === "bigint") {
      stableIds.add(record.rowId);
      return `_rowid:${record.rowId}`;
    }
    if (typeof record.rowAddress === "bigint") {
      addresses.add(record.rowAddress);
      return `_rowaddr:${record.rowAddress}`;
    }
    throw new Error("This Function error has no source row identity");
  });
  const identities = (
    [
      ["_rowid", stableIds],
      ["_rowaddr", addresses],
    ] as const
  ).filter(([, values]) => values.size > 0);
  const identityColumns = identities.map(([column]) => column);
  const selected =
    columns ?? (await table.schema()).fields.map((field) => field.name);
  const projection = [...new Set([...selected, ...identityColumns])];
  const filter = identities
    .map(([column, values]) => `${column} IN (${[...values].join(",")})`)
    .join(" OR ");
  const source = await table
    .query()
    .select(projection)
    .where(filter)
    .limit(stableIds.size + addresses.size)
    .toArrow();
  const selectedNames = new Set(selected);
  const inputColumns = source.schema.fields
    .map((field) => field.name)
    .filter(
      (name) =>
        (name !== "_rowid" && name !== "_rowaddr") || selectedNames.has(name),
    );
  const byIdentity = new Map<string, Record<string, unknown>>();
  for (const row of source) {
    if (row === null) continue;
    const input = Object.fromEntries(
      inputColumns.map((name) => [name, row[name]]),
    );
    for (const column of identityColumns) {
      const identity: unknown = row[column];
      if (typeof identity === "bigint") {
        byIdentity.set(`${column}:${identity}`, input);
      }
    }
  }
  return records.map((error, index) => {
    const input = byIdentity.get(keys[index]);
    if (input === undefined) {
      throw new Error(
        "Some failed inputs are no longer addressable in this snapshot; refresh the error listing",
      );
    }
    return { input, error };
  });
}
