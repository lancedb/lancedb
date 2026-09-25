// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors
import { expect, jest, test } from "@jest/globals";
import * as path from "node:path";
import { withTempDirectory } from "./util.ts";
// --8<-- [start:connect]
import * as lancedb from "@lancedb/lancedb";

async function connectExample(uri: string) {
  const db = await lancedb.connect(uri);
  return db;
}
// --8<-- [end:connect]

test("connect to a local database", async () => {
  await withTempDirectory(async (tempDir) => {
    const uri = path.join(tempDir, "ex_lancedb");
    const db = await connectExample(uri);
    expect(db).toBeDefined();
  });
});

async function connectEnterpriseQuickstart() {
  // --8<-- [start:connect_enterprise_quickstart]
  const uri = "db://your-database-uri";
  const apiKey = "your-api-key";
  const region = "us-east-1";
  const hostOverride = "https://your-enterprise-endpoint.com";

  const db = await lancedb.connect(uri, {
    apiKey,
    region,
    hostOverride,
  });
  // --8<-- [end:connect_enterprise_quickstart]
  return db;
}

test("enterprise quickstart connect uses placeholder config", async () => {
  const mockDb = { __mock: true } as unknown as Awaited<
    ReturnType<typeof lancedb.connect>
  >;
  const spy = jest.spyOn(lancedb, "connect").mockResolvedValue(mockDb);

  const db = await connectEnterpriseQuickstart();
  expect(db).toBe(mockDb);
  expect(spy).toHaveBeenCalledWith("db://your-database-uri", {
    apiKey: "your-api-key",
    region: "us-east-1",
    hostOverride: "https://your-enterprise-endpoint.com",
  });
  spy.mockRestore();
});

// --8<-- [start:connect_object_storage]
async function connectObjectStorageExample() {
  const uri = "s3://your-bucket/path";
  // You can also use "gs://your-bucket/path" or "az://your-container/path".
  const db = await lancedb.connect(uri);
  return db;
}
// --8<-- [end:connect_object_storage]

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
  console.log((await db.listNamespaces()).namespaces); // ["prod"]
  console.log((await db.listNamespaces(["prod"])).namespaces); // ["recommendations", "search"]
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

  const childNamespaces = (await db.listNamespaces(["prod"])).namespaces;
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
