// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { connect } from "../lancedb";
import { experimentalFunctionsEnabled } from "../lancedb/native";

const defaultBuildTest = experimentalFunctionsEnabled() ? it.skip : it;

defaultBuildTest(
  "rejects local Functions before mutating the database",
  async () => {
    const directory = await mkdtemp(join(tmpdir(), "lancedb-experimental-"));
    try {
      const db = await connect(directory);
      const table = await db.createTable("source", [{ x: 1 }, { x: 2 }]);
      const version = await table.version();
      const operations = [
        () =>
          table.addColumns({
            computed: [{ name: "doubled", valueSql: "x * 2" }],
          }),
        () => table.refreshColumn("x"),
        () => table.refreshColumnAsync("x"),
        () => db.createMaterializedView("blocked", "source"),
        () =>
          db.createMaterializedView("blocked", "source", { withNoData: true }),
      ];
      for (const operation of operations) {
        await expect(operation()).rejects.toThrow("experimental-functions");
      }
      expect(await table.version()).toBe(version);
      expect(await db.tableNames()).toEqual(["source"]);
      await table.addColumns([{ name: "eager", valueSql: "x * 2" }]);
      expect((await table.query().toArray()).map((row) => row.eager)).toEqual([
        2, 4,
      ]);
    } finally {
      await rm(directory, { recursive: true, force: true });
    }
  },
);
