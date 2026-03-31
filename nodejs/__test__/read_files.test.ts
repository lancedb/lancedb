// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import * as fs from "fs";
import * as path from "path";
import * as tmp from "tmp";

import { FileSource, connect, readFiles } from "../lancedb";
import { makeArrowTable } from "../lancedb/arrow";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function writeCsv(dir: string, filename: string): string {
  const p = path.join(dir, filename);
  fs.writeFileSync(p, "id,name\n1,a\n2,b\n3,c\n");
  return p;
}

async function writeLance(dir: string): Promise<string> {
  const db = await connect(dir);
  const data = makeArrowTable([
    { id: 1, name: "a" },
    { id: 2, name: "b" },
    { id: 3, name: "c" },
  ]);
  await db.createTable("source", data);
  // createTable writes a .lance directory under dir/source.lance
  return path.join(dir, "source.lance");
}

// ---------------------------------------------------------------------------
// readFiles()
// ---------------------------------------------------------------------------

test("readFiles() returns a FileSource with correct schema for CSV", async () => {
  const dir = tmp.dirSync({ unsafeCleanup: true });
  const csvPath = writeCsv(dir.name, "test.csv");

  const source = await readFiles(csvPath);

  expect(source).toBeInstanceOf(FileSource);
  // CSV infers all columns — just verify there are 2 fields
  expect(source.schema.fields.length).toBe(2);
});

test("readFiles() returns a FileSource with correct schema for Lance", async () => {
  const dir = tmp.dirSync({ unsafeCleanup: true });
  const lancePath = await writeLance(dir.name);

  const source = await readFiles(lancePath);

  expect(source).toBeInstanceOf(FileSource);
  expect(source.schema.fields.length).toBe(2);
  expect(source.schema.fields.map((f) => f.name)).toContain("id");
  expect(source.schema.fields.map((f) => f.name)).toContain("name");
});

test("readFiles() rejects unknown file extensions", async () => {
  await expect(readFiles("data.json")).rejects.toThrow();
});

test("readFiles() rejects Lance glob patterns", async () => {
  await expect(readFiles("./data/*.lance")).rejects.toThrow();
});

// ---------------------------------------------------------------------------
// table.add(fileSource)
// ---------------------------------------------------------------------------

test("table.add() accepts a CSV FileSource", async () => {
  const dir = tmp.dirSync({ unsafeCleanup: true });
  const csvPath = writeCsv(dir.name, "data.csv");

  const db = await connect(path.join(dir.name, "db"));
  const source = await readFiles(csvPath);
  const table = await db.createEmptyTable("test", source.schema);
  await table.add(source);

  expect(await table.countRows()).toBe(3);
});

test("table.add() accepts a Lance FileSource", async () => {
  const dir = tmp.dirSync({ unsafeCleanup: true });
  const lancePath = await writeLance(path.join(dir.name, "src"));

  const db = await connect(path.join(dir.name, "db"));
  const source = await readFiles(lancePath);
  const table = await db.createEmptyTable("test", source.schema);
  await table.add(source);

  expect(await table.countRows()).toBe(3);
});

test("table.add() appends from FileSource", async () => {
  const dir = tmp.dirSync({ unsafeCleanup: true });
  const csvPath = writeCsv(dir.name, "data.csv");

  const db = await connect(path.join(dir.name, "db"));
  const data = makeArrowTable([
    { id: 10, name: "x" },
    { id: 11, name: "y" },
  ]);
  const table = await db.createTable("test", data);

  const source = await readFiles(csvPath);
  await table.add(source);

  expect(await table.countRows()).toBe(5); // 2 initial + 3 from CSV
});
