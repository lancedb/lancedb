// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import { Schema, tableFromIPC } from "apache-arrow";
import { ViewDescription as NativeViewDescription } from "./native";

/**
 * What a database records about one view.
 *
 * A view holds no rows: it stores the statement that defines it and the
 * schema that statement resolved to, so a reader sees its sources as they
 * are at read time.
 */
export interface ViewDescription {
  /** The view's name within its namespace. */
  name: string;
  /** The namespace holding the view; empty is the root namespace. */
  namespacePath: string[];
  /** The defining query, as the database stores it. */
  query: string;
  /** The database that unqualified table names in {@link query} resolve against. */
  defaultDatabase: string;
  /** The schema the defining query resolved to when the view was created. */
  schema: Schema;
}

/** Decode the schema the binding hands over as an Arrow IPC file. */
export function viewDescriptionFromNative(
  view: NativeViewDescription,
): ViewDescription {
  return {
    name: view.name,
    namespacePath: view.namespacePath,
    query: view.query,
    defaultDatabase: view.defaultDatabase,
    schema: tableFromIPC(view.schema).schema,
  };
}
