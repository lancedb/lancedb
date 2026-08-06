// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");

const { connect } = require("../dist");

async function main() {
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "lancedb-cpu-"));

  try {
    const db = await connect(tmpDir);
    const table = await db.createTable("vectors", [
      { id: 1, vector: [1, 0, 0] },
      { id: 2, vector: [0, 1, 0] },
    ]);
    const results = await table
      .vectorSearch([1, 0, 0])
      .distanceType("cosine")
      .limit(1)
      .toArray();

    assert.equal(results.length, 1);
    assert.equal(results[0].id, 1);
    assert.ok(Math.abs(results[0]._distance) < 1e-6);
  } finally {
    fs.rmSync(tmpDir, { force: true, recursive: true });
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
