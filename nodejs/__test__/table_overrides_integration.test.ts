// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import { randomUUID } from "node:crypto";
import { Connection, Table, connect } from "../lancedb";

// Run against an isolated Enterprise deployment:
// LANCEDB_TABLE_OVERRIDES_E2E=1 LANCEDB_URI=db://test \
// LANCEDB_HOST_OVERRIDE=http://localhost:10024 LANCEDB_API_KEY=... \
// pnpm test __test__/table_overrides_integration.test.ts --runInBand
const maybeDescribe = process.env.LANCEDB_TABLE_OVERRIDES_E2E
  ? describe
  : describe.skip;

maybeDescribe("table overrides live integration", () => {
  let db: Connection;
  const namespace = [`overrides_${randomUUID().replaceAll("-", "_")}`];
  const nested = [...namespace, "nested"];
  const uri = process.env.LANCEDB_URI ?? "db://test";
  const host = process.env.LANCEDB_HOST_OVERRIDE;
  const apiKey = process.env.LANCEDB_API_KEY;

  beforeAll(async () => {
    if (!host || !apiKey) {
      throw new Error(
        "Set LANCEDB_HOST_OVERRIDE and LANCEDB_API_KEY for live tests",
      );
    }
    db = await connect(uri, { apiKey, hostOverride: host });
    await db.createNamespace(namespace);
    await db.createNamespace(nested);
  });

  afterAll(async () => {
    if (db) {
      try {
        await db.dropNamespace(nested);
        await db.dropNamespace(namespace);
      } finally {
        db.close();
      }
    }
  });

  async function withTable(
    namespacePath: string[],
    run: (table: Table, name: string) => Promise<void>,
  ) {
    const name = `items_${randomUUID().replaceAll("-", "_")}`;
    const table = await db.createTable(name, [{ id: 1 }], namespacePath);
    try {
      await run(table, name);
    } finally {
      table.close();
      await db.dropTable(name, namespacePath);
    }
  }

  it.each([
    { label: "root", path: [] },
    { label: "nested", path: nested },
  ])(
    "merges, replaces via REST, and resets in $label namespace",
    async ({ path }) => {
      await withTable(path, async (table, name) => {
        await expect(table.getTableOverrides()).resolves.toEqual({});
        const initial = {
          // biome-ignore lint/style/useNamingConvention: server wire format
          job_types: {
            compaction: { enabled: false },
            cleanup: { enabled: false },
          },
          custom: { keep: "測試", values: [1, 2], nullable: "old" },
        };
        await expect(table.updateTableOverrides(initial)).resolves.toEqual(
          initial,
        );
        const patch = {
          // biome-ignore lint/style/useNamingConvention: server wire format
          job_types: { compaction: { enabled: true } },
          custom: { values: [3], nullable: null },
        };
        const expected = {
          // biome-ignore lint/style/useNamingConvention: server wire format
          job_types: {
            compaction: { enabled: true },
            cleanup: { enabled: false },
          },
          custom: { keep: "測試", values: [3], nullable: null },
        };
        await expect(table.updateTableOverrides(patch)).resolves.toEqual(
          expected,
        );
        await expect(table.getTableOverrides()).resolves.toEqual(expected);
        await expect(table.updateTableOverrides({})).resolves.toEqual(expected);

        const replacement = { custom: { replacement: true } };
        const response = await fetch(`${host}/admin/table/overrides`, {
          method: "PUT",
          headers: { "x-api-key": apiKey!, "Content-Type": "application/json" },
          body: JSON.stringify({
            db: new URL(uri).hostname,
            namespace: path,
            table: name,
            overrides: replacement,
          }),
        });
        expect(response.status).toBe(200);
        expect(await response.json()).toEqual(replacement);
        await expect(table.getTableOverrides()).resolves.toEqual(replacement);
        await expect(table.resetTableOverrides()).resolves.toEqual({});
        await expect(table.getTableOverrides()).resolves.toEqual({});
        await expect(table.countRows()).resolves.toBe(1);
      });
    },
  );

  it("isolates branch updates and resets from main", async () => {
    await withTable(nested, async (table) => {
      const initial = { custom: { main: true } };
      await table.updateTableOverrides(initial);
      const branch = await (await table.branches()).create("experiment");
      try {
        const expected = { custom: { main: true, branch: true } };
        await expect(
          branch.updateTableOverrides({ custom: { branch: true } }),
        ).resolves.toEqual(expected);
        await expect(table.getTableOverrides()).resolves.toEqual(initial);
        await expect(branch.resetTableOverrides()).resolves.toEqual({});
        await expect(branch.getTableOverrides()).resolves.toEqual({});
        await expect(table.getTableOverrides()).resolves.toEqual(initial);
      } finally {
        branch.close();
      }
    });
  });

  it("propagates server validation errors without changing stored overrides", async () => {
    await withTable(nested, async (table) => {
      const initial = { custom: { keep: true } };
      await table.updateTableOverrides(initial);
      await expect(
        table.updateTableOverrides({
          // biome-ignore lint/style/useNamingConvention: server wire format
          auto_refresh_interval_seconds: -1,
        }),
      ).rejects.toThrow();
      await expect(table.getTableOverrides()).resolves.toEqual(initial);
    });
  });

  it("rejects writes at a fixed version", async () => {
    await withTable(nested, async (table) => {
      await table.checkout(1);
      await expect(
        table.updateTableOverrides({ custom: true }),
      ).rejects.toThrow(/fixed at version/);
      await expect(table.resetTableOverrides()).rejects.toThrow(
        /fixed at version/,
      );
      await table.checkoutLatest();
      await expect(table.getTableOverrides()).resolves.toEqual({});
    });
  });
});
