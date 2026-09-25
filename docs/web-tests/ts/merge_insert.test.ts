// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import { expect, test } from "@jest/globals";
import * as lancedb from "@lancedb/lancedb";

test("basic upsert", async () => {
  const db = await lancedb.connect("memory://");

  // --8<-- [start:upsert_basic]
  const table = await db.createTable("users", [
    { id: 0, name: "Alice" },
    { id: 1, name: "Bob" },
  ]);

  const newUsers = [
    { id: 1, name: "Bobby" },
    { id: 2, name: "Charlie" },
  ];
  await table
    .mergeInsert("id")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute(newUsers);

  await table.countRows(); // 3
  // --8<-- [end:upsert_basic]
  expect(await table.countRows()).toBe(3);

  // --8<-- [start:insert_if_not_exists]
  const table2 = await db.createTable("domains", [
    { domain: "google.com", name: "Google" },
    { domain: "github.com", name: "GitHub" },
  ]);

  const newDomains = [
    { domain: "google.com", name: "Google" },
    { domain: "facebook.com", name: "Facebook" },
  ];
  await table2
    .mergeInsert("domain")
    .whenNotMatchedInsertAll()
    .execute(newDomains);
  await table2.countRows(); // 3
  // --8<-- [end:insert_if_not_exists]
  expect(await table2.countRows()).toBe(3);

  // --8<-- [start:replace_range]
  const table3 = await db.createTable("chunks", [
    { doc_id: 0, chunk_id: 0, text: "Hello" },
    { doc_id: 0, chunk_id: 1, text: "World" },
    { doc_id: 1, chunk_id: 0, text: "Foo" },
    { doc_id: 1, chunk_id: 1, text: "Bar" },
  ]);

  const newChunks = [{ doc_id: 1, chunk_id: 0, text: "Baz" }];

  await table3
    .mergeInsert(["doc_id", "chunk_id"])
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .whenNotMatchedBySourceDelete({ where: "doc_id = 1" })
    .execute(newChunks);

  await table3.countRows("doc_id = 1"); // 1
  // --8<-- [end:replace_range]
  expect(await table3.countRows("doc_id = 1")).toBe(1);
});
