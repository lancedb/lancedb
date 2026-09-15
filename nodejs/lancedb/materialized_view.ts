// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import { RefreshMaterializedViewResult } from "./native";
import { Table } from "./table";

/** Schema metadata key holding a materialized view's definition. */
export const DEFINITION_META_KEY = "mv.definition";

/** The stored layout this version reads: `{"format": 1, "query": "<SQL>"}`. */
export const DEFINITION_FORMAT = 1;

/**
 * The query that defines a materialized view, as stored:
 * `SELECT columns FROM [ns.]table [, UNNEST(column) AS alias] [WHERE predicate] [LIMIT n]`.
 */
export interface MaterializedViewDefinition {
  /** The defining query, in the canonical spelling the server stores. */
  query: string;
}

/**
 * The view's columns: column names, `[alias, SQL expression]` pairs, or a
 * record of the same. A bare name projects itself.
 */
export type MaterializedViewSelect =
  | (string | [string, string])[]
  | Record<string, string>;

/**
 * @internal Reject a numeric option N-API would otherwise silently coerce:
 * `Infinity` reaches Rust as 0, `1.5` as 1.
 */
export function validateNonNegativeInteger(
  value: number | undefined,
  name: string,
): void {
  if (value !== undefined && !(Number.isSafeInteger(value) && value >= 0)) {
    throw new Error(`${name} must be a non-negative integer`);
  }
}

/** @internal Quote a column name as a Lance SQL identifier (backticks). */
function quoteIdentifier(name: string): string {
  return "`" + name.replace(/`/g, "``") + "`";
}

/**
 * @internal Normalize a select argument into `[alias, expression]` pairs.
 * A bare name projects itself and is quoted, so any valid column name works;
 * pair and record entries are kept verbatim because their right side is an
 * expression.
 */
export function normalizeSelect(
  select?: MaterializedViewSelect,
): [string, string][] | undefined {
  if (select === undefined) {
    return undefined;
  }
  if (Array.isArray(select)) {
    return select.map((item) =>
      typeof item === "string" ? [item, quoteIdentifier(item)] : item,
    );
  }
  return Object.entries(select);
}

/** @internal Parse a definition off a table's stored schema metadata. */
export function definitionFromMetadata(
  metadata: Map<string, string>,
  name: string,
): MaterializedViewDefinition {
  const raw = metadata.get(DEFINITION_META_KEY);
  if (raw === undefined) {
    throw new Error(`Table '${name}' is not a materialized view`);
  }
  // biome-ignore lint/suspicious/noExplicitAny: raw JSON
  const value: any = JSON.parse(raw);
  if (value.format !== undefined) {
    // A newer writer's layout is reported, never guessed at.
    if (!Number.isInteger(value.format) || value.format > DEFINITION_FORMAT) {
      throw new Error(
        `materialized view '${name}' is stored in format ${value.format}, ` +
          "which this version of lancedb cannot refresh",
      );
    }
    return { query: value.query };
  }
  // The structured layout written before the format number.
  if (value.kind !== "select" && value.kind !== "namespaced_select") {
    throw new Error(
      `materialized view '${name}' is stored in format kind '${value.kind}', ` +
        "which this version of lancedb cannot refresh",
    );
  }
  const limit = value.limit ?? undefined;
  // JSON.parse rounds integers past 2^53; every exact u64 parses to a safe
  // integer and every rounded one does not, so this rejects precisely the
  // values a number cannot carry.
  if (limit !== undefined && !Number.isSafeInteger(limit)) {
    throw new Error(
      `materialized view '${name}' has a stored limit too large to represent exactly`,
    );
  }
  return { query: legacyQuery(value, limit) };
}

function legacyIdent(name: string): string {
  return /^[a-z_][a-z0-9_]*$/.test(name)
    ? name
    : `\`${name.replace(/`/g, "``")}\``;
}

/** Render the pre-format structured layout as the query it described. */
// biome-ignore lint/suspicious/noExplicitAny: raw JSON
function legacyQuery(value: any, limit: number | undefined): string {
  // biome-ignore lint/suspicious/noExplicitAny: raw JSON
  const projections: any[] = value.projections ?? [];
  const columns =
    projections.length === 0
      ? "*"
      : projections
          .map((p) =>
            p.expression === p.output || p.expression === `\`${p.output}\``
              ? p.expression
              : `${p.expression} AS ${legacyIdent(p.output)}`,
          )
          .join(", ");
  const table = [...(value.source_namespace ?? []), value.source_table]
    .map(legacyIdent)
    .join(".");
  let query = `SELECT ${columns} FROM ${table}`;
  if (value.filter !== undefined && value.filter !== null) {
    query += ` WHERE ${value.filter}`;
  }
  if (limit !== undefined) {
    query += ` LIMIT ${limit}`;
  }
  return query;
}

/**
 * A handle on a materialized view: its table plus its definition.
 *
 * Obtained from {@link Connection#createMaterializedView} or
 * {@link Connection#openMaterializedView}. The view is a normal table --
 * queries, indexes and search all apply through {@link MaterializedView#table}
 * -- whose contents are maintained by {@link MaterializedView#refresh}.
 */
export class MaterializedView {
  private readonly inner: Table;

  constructor(table: Table) {
    this.inner = table;
  }

  get name(): string {
    return this.inner.name;
  }

  /** The view, as the table it is. */
  table(): Table {
    return this.inner;
  }

  /** The query that defines the view, read from its stored schema. */
  async definition(): Promise<MaterializedViewDefinition> {
    const schema = await this.inner.schema();
    return definitionFromMetadata(schema.metadata, this.name);
  }

  /**
   * Recompute the view from its source.
   *
   * The refresh is incremental when the source's changes can be reconciled
   * into the view -- rows added, changed or removed since the last one --
   * and otherwise rebuilds. `full` forces a rebuild; `sourceVersion`
   * refreshes to that source version instead of the latest.
   *
   * Concurrent refreshes of one view do not duplicate its rows. Two that
   * plan the same source rows conflict on commit, and the loser throws
   * rather than writing them a second time.
   */
  async refresh(options?: {
    full?: boolean;
    sourceVersion?: number;
  }): Promise<RefreshMaterializedViewResult> {
    validateNonNegativeInteger(options?.sourceVersion, "sourceVersion");
    return await this.inner.refreshMaterializedView(
      options?.full,
      options?.sourceVersion,
    );
  }
}
