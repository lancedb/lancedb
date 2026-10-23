// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors
import { afterEach, describe, expect, jest, test } from "@jest/globals";
import * as lancedb from "@lancedb/lancedb";
import { withTempDirectory } from "./util.ts";

async function collectNames(items: AsyncIterable<string>): Promise<string[]> {
  const names: string[] = [];
  for await (const name of items) names.push(name);
  return names;
}

test("connect to a local database", async () => {
  await withTempDirectory(async (tempDir) => {
    const cwd = process.cwd();
    process.chdir(tempDir);
    try {
      // --8<-- [start:connect]
      const db = await lancedb.connect("ex_lancedb");
      // --8<-- [end:connect]
      await db.createTable("probe", [{ id: 1 }]);
      expect(await collectNames(db.listTables())).toEqual(["probe"]);
    } finally {
      process.chdir(cwd);
    }
    const reopened = await lancedb.connect(`${tempDir}/ex_lancedb`);
    expect(await collectNames(reopened.listTables())).toEqual(["probe"]);
  });
});

async function connectEnterpriseQuickstart() {
  // --8<-- [start:connect_enterprise_quickstart]
  const setting = (name: string) => {
    const value = process.env[name];
    if (!value) throw new Error(`Set ${name}`);
    return value;
  };

  const db = await lancedb.connect(setting("LANCEDB_URI"), {
    apiKey: setting("LANCEDB_API_KEY"),
    region: process.env.LANCEDB_REGION ?? "us-east-1",
    hostOverride: setting("LANCEDB_HOST_OVERRIDE"),
  });
  // --8<-- [end:connect_enterprise_quickstart]
  return db;
}

describe("enterprise quickstart connect", () => {
  const saved = { ...process.env };
  afterEach(() => {
    process.env = { ...saved };
  });

  test("reads the connection from the environment", async () => {
    const mockDb = { __mock: true } as unknown as Awaited<
      ReturnType<typeof lancedb.connect>
    >;
    const spy = jest.spyOn(lancedb, "connect").mockResolvedValue(mockDb);
    process.env.LANCEDB_URI = "db://your-database";
    process.env.LANCEDB_API_KEY = "your-api-key";
    delete process.env.LANCEDB_REGION;
    process.env.LANCEDB_HOST_OVERRIDE = "https://your-endpoint";

    const db = await connectEnterpriseQuickstart();
    expect(db).toBe(mockDb);
    expect(spy).toHaveBeenCalledWith("db://your-database", {
      apiKey: "your-api-key",
      region: "us-east-1",
      hostOverride: "https://your-endpoint",
    });
    spy.mockRestore();
  });

  test("requires an endpoint", async () => {
    process.env.LANCEDB_URI = "db://your-database";
    process.env.LANCEDB_API_KEY = "your-api-key";
    delete process.env.LANCEDB_HOST_OVERRIDE;

    await expect(connectEnterpriseQuickstart()).rejects.toThrow(
      "Set LANCEDB_HOST_OVERRIDE",
    );
  });
});

async function connectObjectStorageExample() {
  // --8<-- [start:connect_object_storage]
  const uri = "s3://your-bucket/path";
  // You can also use "gs://your-bucket/path" or "az://your-container/path".
  const db = await lancedb.connect(uri);
  // --8<-- [end:connect_object_storage]
  return db;
}

async function namespaceTableOpsExample() {
  // --8<-- [start:namespace_table_ops]
  const db = await lancedb.connectNamespace("dir", { root: "./local_lancedb" });

  // Create namespace tree: prod/search and prod/recommendations
  await db.createNamespace(["prod"], { mode: "exist_ok" });
  await db.createNamespace(["prod", "search"], { mode: "exist_ok" });
  await db.createNamespace(["prod", "recommendations"], { mode: "exist_ok" });

  await db.createTable(
    "user",
    [{ id: 1, vector: [0.1, 0.2], name: "alice" }],
    ["prod", "search"],
    { mode: "create" }, // use "overwrite" only if you want to replace existing table
  );

  await db.createTable(
    "user",
    [{ id: 2, vector: [0.3, 0.4], name: "bob" }],
    ["prod", "recommendations"],
    { mode: "create" },
  );

  // Verify
  console.log(await collectNames(db.listNamespaces())); // ["prod"]
  console.log(await collectNames(db.listNamespaces(["prod"]))); // ["recommendations", "search"]
  console.log(await db.tableNames(["prod", "search"])); // ["user"]
  console.log(await db.tableNames(["prod", "recommendations"])); // ["user"]
  // --8<-- [end:namespace_table_ops]
}

async function namespaceAdminOpsExample() {
  // --8<-- [start:namespace_admin_ops]
  const db = await lancedb.connectNamespace("dir", { root: "./local_lancedb" });
  const namespace = ["prod", "search"];

  await db.createNamespace(["prod"]);
  await db.createNamespace(["prod", "search"]);

  const childNamespaces = await collectNames(db.listNamespaces(["prod"]));
  console.log(`Child namespaces under ${JSON.stringify(namespace)}:`, childNamespaces);
  // Child namespaces under ["prod","search"]: [ 'search' ]

  const metadata = await db.describeNamespace(["prod", "search"]);
  console.log(`Metadata for namespace ${JSON.stringify(namespace)}:`, metadata);

  await db.dropNamespace(["prod", "search"], { mode: "skip" });
  await db.dropNamespace(["prod"], { mode: "skip" });
  // --8<-- [end:namespace_admin_ops]
  return { childNamespaces, metadata };
}

void [
  connectObjectStorageExample,
  connectEnterpriseQuickstart,
  namespaceTableOpsExample,
  namespaceAdminOpsExample,
];

test("enterprise placeholders use an explicit endpoint", async () => {
  const mockDb = {} as Awaited<ReturnType<typeof lancedb.connect>>;
  const spy = jest.spyOn(lancedb, "connect").mockResolvedValue(mockDb);
  try {
    // --8<-- [start:connect_enterprise_placeholders]
    const db = await lancedb.connect("db://your-database", {
      apiKey: "your-api-key",
      region: "us-east-1",
      hostOverride: "https://your-endpoint",
    });
    // --8<-- [end:connect_enterprise_placeholders]
    expect(db).toBe(mockDb);
    expect(spy).toHaveBeenCalledWith("db://your-database", {
      apiKey: "your-api-key",
      region: "us-east-1",
      hostOverride: "https://your-endpoint",
    });
  } finally {
    spy.mockRestore();
  }
});
