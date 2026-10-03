// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

const assert = require("node:assert/strict");
const tmp = require("tmp");
const { connect, embedding, Index } = require("../../dist");
const { getRegistry } = require("../../dist/embedding/registry");

async function main() {
  assert.equal(typeof embedding.getRegistry, "function");
  assert.equal(getRegistry().length(), 0);
  assert.equal(embedding.getRegistry(), getRegistry());
  assert.equal(getRegistry().length(), 2);

  const dir = tmp.dirSync({ unsafeCleanup: true });
  let db;
  try {
    db = await connect(dir.name);
    const table = await db.createTable("docs", [{ text: "hello world" }]);
    await table.createIndex("text", { config: Index.fts() });

    const rows = await table.search("hello").toArray();
    assert.equal(rows[0].text, "hello world");
  } finally {
    db?.close();
    dir.removeCallback();
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
