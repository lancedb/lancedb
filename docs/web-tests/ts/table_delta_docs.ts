// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import type { Connection } from "@lancedb/lancedb";

/**
 * Draft checkpoint-delta example for the Versioning guide.
 *
 * This file is snippet source only. The published documentation test package is
 * pinned to an older npm release, so it is not part of the Jest test glob.
 */
export async function versioningCheckpointDeltaExample(
  db: Connection,
): Promise<void> {
  // --8<-- [start:versioning_checkpoint_delta]
  const table = await db.createTable(
    "checkpoint_delta_example",
    [
      { id: 1, author: "Richard", quote: "Wubba Lubba Dub Dub!" },
      { id: 2, author: "Morty", quote: "Rick, what's going on?" },
    ],
    {
      mode: "overwrite",
      storageOptions: { new_table_enable_stable_row_ids: "true" },
    },
  );
  const tags = await table.tags();
  await tags.create("baseline", await table.version());
  await table.add([
    { id: 3, author: "Summer", quote: "Whatever, Grandpa" },
  ]);
  await table.update({
    where: "id = 2",
    values: { quote: "Aww geez, Rick!" },
  });
  await table.delete("id = 1");

  const delta = await table.delta("baseline");
  console.log(delta.beginVersion, delta.endVersion);

  const inserted = delta.insertedRows();
  console.log((await inserted.schema()).fields.map((field) => field.name));
  for await (const batch of inserted) {
    console.log("inserted:", batch.getChild("id")?.toJSON());
  }

  const updated = delta.updatedRows();
  for await (const batch of updated) {
    console.log("updated:", batch.getChild("id")?.toJSON());
  }

  const deleted = delta.deletedRowIds();
  for await (const batch of deleted) {
    console.log("deleted row ids:", batch.getChild("_rowid")?.toJSON());
  }

  console.log(
    "transactions:",
    (await delta.listTransactions()).map(
      (transaction) => transaction.operation,
    ),
  );
  // --8<-- [end:versioning_checkpoint_delta]
}
