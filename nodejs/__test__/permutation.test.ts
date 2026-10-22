// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import * as tmp from "tmp";
import { Table, connect, permutationBuilder } from "../lancedb";
import { makeArrowTable } from "../lancedb/arrow";

describe("PermutationBuilder", () => {
  let tmpDir: tmp.DirResult;
  let table: Table;

  beforeEach(async () => {
    tmpDir = tmp.dirSync({ unsafeCleanup: true });
    const db = await connect(tmpDir.name);

    // Create test data
    const data = makeArrowTable(
      [
        { id: 1, value: 10 },
        { id: 2, value: 20 },
        { id: 3, value: 30 },
        { id: 4, value: 40 },
        { id: 5, value: 50 },
        { id: 6, value: 60 },
        { id: 7, value: 70 },
        { id: 8, value: 80 },
        { id: 9, value: 90 },
        { id: 10, value: 100 },
      ],
      { vectorColumns: {} },
    );

    table = await db.createTable("test_table", data);
  });

  afterEach(() => {
    tmpDir.removeCallback();
  });

  test("should create permutation builder", () => {
    const builder = permutationBuilder(table);
    expect(builder).toBeDefined();
  });

  test("should execute basic permutation", async () => {
    const builder = permutationBuilder(table);
    const permutationTable = await builder.execute();

    expect(permutationTable).toBeDefined();

    const rowCount = await permutationTable.countRows();
    expect(rowCount).toBe(10);
  });

  test("should create permutation with random splits", async () => {
    const builder = permutationBuilder(table).splitRandom({
      ratios: [1.0],
      seed: 42,
    });

    const permutationTable = await builder.execute();
    const rowCount = await permutationTable.countRows();
    expect(rowCount).toBe(10);
  });

  test("should create permutation with percentage splits", async () => {
    const builder = permutationBuilder(table).splitRandom({
      ratios: [0.3, 0.7],
      seed: 42,
    });

    const permutationTable = await builder.execute();
    const rowCount = await permutationTable.countRows();
    expect(rowCount).toBe(10);

    // Check split distribution
    const split0Count = await permutationTable.countRows("split_id = 0");
    const split1Count = await permutationTable.countRows("split_id = 1");

    expect(split0Count).toBeGreaterThan(0);
    expect(split1Count).toBeGreaterThan(0);
    expect(split0Count + split1Count).toBe(10);
  });

  test("should create permutation with count splits", async () => {
    const builder = permutationBuilder(table).splitRandom({
      counts: [3, 7],
      seed: 42,
    });

    const permutationTable = await builder.execute();
    const rowCount = await permutationTable.countRows();
    expect(rowCount).toBe(10);

    // Check split distribution
    const split0Count = await permutationTable.countRows("split_id = 0");
    const split1Count = await permutationTable.countRows("split_id = 1");

    expect(split0Count).toBe(3);
    expect(split1Count).toBe(7);
  });

  test("should create permutation with hash splits", async () => {
    const builder = permutationBuilder(table).splitHash({
      columns: ["id"],
      splitWeights: [50, 50],
      discardWeight: 0,
    });

    const permutationTable = await builder.execute();
    const rowCount = await permutationTable.countRows();
    expect(rowCount).toBe(10);

    // Check that splits exist
    const split0Count = await permutationTable.countRows("split_id = 0");
    const split1Count = await permutationTable.countRows("split_id = 1");

    expect(split0Count).toBeGreaterThan(0);
    expect(split1Count).toBeGreaterThan(0);
    expect(split0Count + split1Count).toBe(10);
  });

  test("should create permutation with sequential splits", async () => {
    const builder = permutationBuilder(table).splitSequential({
      ratios: [0.5, 0.5],
    });

    const permutationTable = await builder.execute();
    const rowCount = await permutationTable.countRows();
    expect(rowCount).toBe(10);

    // Check split distribution - sequential should give exactly 5 and 5
    const split0Count = await permutationTable.countRows("split_id = 0");
    const split1Count = await permutationTable.countRows("split_id = 1");

    expect(split0Count).toBe(5);
    expect(split1Count).toBe(5);
  });

  test("should create permutation with calculated splits", async () => {
    const builder = permutationBuilder(table).splitCalculated({
      calculation: "id % 2",
    });

    const permutationTable = await builder.execute();
    const rowCount = await permutationTable.countRows();
    expect(rowCount).toBe(10);

    // Check split distribution
    const split0Count = await permutationTable.countRows("split_id = 0");
    const split1Count = await permutationTable.countRows("split_id = 1");

    expect(split0Count).toBeGreaterThan(0);
    expect(split1Count).toBeGreaterThan(0);
    expect(split0Count + split1Count).toBe(10);
  });

  test("should create permutation with shuffle", async () => {
    const builder = permutationBuilder(table).shuffle({
      seed: 42,
    });

    const permutationTable = await builder.execute();
    const rowCount = await permutationTable.countRows();
    expect(rowCount).toBe(10);
  });

  test("should create permutation with shuffle and clump size", async () => {
    const builder = permutationBuilder(table).shuffle({
      seed: 42,
      clumpSize: 2,
    });

    const permutationTable = await builder.execute();
    const rowCount = await permutationTable.countRows();
    expect(rowCount).toBe(10);
  });

  test("should create permutation with filter", async () => {
    const builder = permutationBuilder(table).filter("value > 50");

    const permutationTable = await builder.execute();
    const rowCount = await permutationTable.countRows();
    expect(rowCount).toBe(5); // Values 60, 70, 80, 90, 100
  });

  test("should chain multiple operations", async () => {
    const builder = permutationBuilder(table)
      .filter("value <= 80")
      .splitRandom({ ratios: [0.5, 0.5], seed: 42 })
      .shuffle({ seed: 123 });

    const permutationTable = await builder.execute();
    const rowCount = await permutationTable.countRows();
    expect(rowCount).toBe(8); // Values 10, 20, 30, 40, 50, 60, 70, 80

    // Check split distribution
    const split0Count = await permutationTable.countRows("split_id = 0");
    const split1Count = await permutationTable.countRows("split_id = 1");

    expect(split0Count).toBeGreaterThan(0);
    expect(split1Count).toBeGreaterThan(0);
    expect(split0Count + split1Count).toBe(8);
  });

  test("should throw error for invalid split arguments", () => {
    const builder = permutationBuilder(table);

    // Test no arguments provided
    expect(() => builder.splitRandom({})).toThrow(
      "Exactly one of 'ratios', 'counts', or 'fixed' must be provided",
    );

    // Test multiple arguments provided
    expect(() =>
      builder.splitRandom({ ratios: [0.5, 0.5], counts: [3, 7], seed: 42 }),
    ).toThrow("Exactly one of 'ratios', 'counts', or 'fixed' must be provided");
  });

  test("should throw error when builder is consumed", async () => {
    const builder = permutationBuilder(table);

    // Execute once
    await builder.execute();

    // Should throw error on second execution
    await expect(builder.execute()).rejects.toThrow("Builder already consumed");
  });

  test("should accept custom split names with random splits", async () => {
    const builder = permutationBuilder(table).splitRandom({
      ratios: [0.3, 0.7],
      seed: 42,
      splitNames: ["train", "test"],
    });

    const permutationTable = await builder.execute();
    const rowCount = await permutationTable.countRows();
    expect(rowCount).toBe(10);

    // Split names are provided but split_id is still numeric (0, 1, etc.)
    // The names are metadata that can be used by higher-level APIs
    const split0Count = await permutationTable.countRows("split_id = 0");
    const split1Count = await permutationTable.countRows("split_id = 1");

    expect(split0Count).toBeGreaterThan(0);
    expect(split1Count).toBeGreaterThan(0);
    expect(split0Count + split1Count).toBe(10);
  });

  test("should accept custom split names with hash splits", async () => {
    const builder = permutationBuilder(table).splitHash({
      columns: ["id"],
      splitWeights: [50, 50],
      discardWeight: 0,
      splitNames: ["set_a", "set_b"],
    });

    const permutationTable = await builder.execute();
    const rowCount = await permutationTable.countRows();
    expect(rowCount).toBe(10);

    // Split names are provided but split_id is still numeric
    const split0Count = await permutationTable.countRows("split_id = 0");
    const split1Count = await permutationTable.countRows("split_id = 1");

    expect(split0Count).toBeGreaterThan(0);
    expect(split1Count).toBeGreaterThan(0);
    expect(split0Count + split1Count).toBe(10);
  });

  test("should accept custom split names with sequential splits", async () => {
    const builder = permutationBuilder(table).splitSequential({
      ratios: [0.5, 0.5],
      splitNames: ["first", "second"],
    });

    const permutationTable = await builder.execute();
    const rowCount = await permutationTable.countRows();
    expect(rowCount).toBe(10);

    // Split names are provided but split_id is still numeric
    const split0Count = await permutationTable.countRows("split_id = 0");
    const split1Count = await permutationTable.countRows("split_id = 1");

    expect(split0Count).toBe(5);
    expect(split1Count).toBe(5);
  });

  test("should accept custom split names with calculated splits", async () => {
    const builder = permutationBuilder(table).splitCalculated({
      calculation: "id % 2",
      splitNames: ["even", "odd"],
    });

    const permutationTable = await builder.execute();
    const rowCount = await permutationTable.countRows();
    expect(rowCount).toBe(10);

    // Split names are provided but split_id is still numeric
    const split0Count = await permutationTable.countRows("split_id = 0");
    const split1Count = await permutationTable.countRows("split_id = 1");

    expect(split0Count).toBeGreaterThan(0);
    expect(split1Count).toBeGreaterThan(0);
    expect(split0Count + split1Count).toBe(10);
  });

  test("should persist permutation to a new table", async () => {
    const db = await connect(tmpDir.name);
    const builder = permutationBuilder(table)
      .splitRandom({
        ratios: [0.7, 0.3],
        seed: 42,
        splitNames: ["train", "validation"],
      })
      .persist(db, "my_permutation");

    // Execute the builder which will persist the table
    const permutationTable = await builder.execute();

    // Verify the persisted table exists and can be opened
    const persistedTable = await db.openTable("my_permutation");
    expect(persistedTable).toBeDefined();

    // Verify the persisted table has the correct number of rows
    const rowCount = await persistedTable.countRows();
    expect(rowCount).toBe(10);

    // Verify splits exist (numeric split_id values)
    const split0Count = await persistedTable.countRows("split_id = 0");
    const split1Count = await persistedTable.countRows("split_id = 1");

    expect(split0Count).toBeGreaterThan(0);
    expect(split1Count).toBeGreaterThan(0);
    expect(split0Count + split1Count).toBe(10);

    // Verify the table returned by execute is the same as the persisted one
    const executedRowCount = await permutationTable.countRows();
    expect(executedRowCount).toBe(10);
  });

  test("should persist permutation with multiple operations", async () => {
    const db = await connect(tmpDir.name);
    const builder = permutationBuilder(table)
      .filter("value > 30")
      .splitRandom({ ratios: [0.5, 0.5], seed: 123, splitNames: ["a", "b"] })
      .shuffle({ seed: 456 })
      .persist(db, "filtered_permutation");

    // Execute the builder
    const permutationTable = await builder.execute();

    // Verify the persisted table
    const persistedTable = await db.openTable("filtered_permutation");
    const rowCount = await persistedTable.countRows();
    expect(rowCount).toBe(7); // Values 40, 50, 60, 70, 80, 90, 100

    // Verify splits exist (numeric split_id values)
    const split0Count = await persistedTable.countRows("split_id = 0");
    const split1Count = await persistedTable.countRows("split_id = 1");

    expect(split0Count).toBeGreaterThan(0);
    expect(split1Count).toBeGreaterThan(0);
    expect(split0Count + split1Count).toBe(7);

    // Verify the executed table matches
    const executedRowCount = await permutationTable.countRows();
    expect(executedRowCount).toBe(7);
  });
});
