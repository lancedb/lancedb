// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import {
  Field,
  Float16,
  Int32,
  type RecordBatch,
  RecordBatchReader,
  Schema,
  tableToIPC,
} from "apache-arrow";
import { makeArrowTable, makeEmptyTable } from "../lancedb/arrow";
import { Scannable } from "../lancedb/scannable";

function makeTable() {
  return makeArrowTable(
    [
      { id: 1, name: "a" },
      { id: 2, name: "b" },
      { id: 3, name: "c" },
    ],
    { vectorColumns: {} },
  );
}

async function makeReader(): Promise<RecordBatchReader> {
  // `RecordBatchReader.from()` returns an unopened reader; `.schema` is only
  // populated after `.open()`. Opening sync readers is synchronous.
  const reader = RecordBatchReader.from(tableToIPC(makeTable()));
  return reader.open() as RecordBatchReader;
}

describe("Scannable", () => {
  describe("fromTable", () => {
    test("reflects schema, numRows, and defaults rescannable=true", () => {
      const table = makeTable();
      const scannable = Scannable.fromTable(table);

      expect(scannable.schema).toBe(table.schema);
      expect(scannable.numRows).toBe(table.numRows);
      expect(scannable.rescannable).toBe(true);
    });

    test("honors numRows override", () => {
      const scannable = Scannable.fromTable(makeTable(), { numRows: 42 });
      expect(scannable.numRows).toBe(42);
    });

    test("honors rescannable override", () => {
      const scannable = Scannable.fromTable(makeTable(), {
        rescannable: false,
      });
      expect(scannable.rescannable).toBe(false);
    });
  });

  describe("fromRecordBatchReader", () => {
    test("reflects schema and defaults numRows=null, rescannable=false", async () => {
      const reader = await makeReader();
      const scannable = Scannable.fromRecordBatchReader(reader);

      expect(scannable.schema).toBe(reader.schema);
      expect(scannable.numRows).toBeNull();
      expect(scannable.rescannable).toBe(false);
    });

    test("honors numRows and rescannable overrides", async () => {
      const scannable = Scannable.fromRecordBatchReader(await makeReader(), {
        numRows: 3,
        rescannable: true,
      });

      expect(scannable.numRows).toBe(3);
      expect(scannable.rescannable).toBe(true);
    });
  });

  describe("fromIterable", () => {
    test("accepts a sync iterable of batches", () => {
      const table = makeTable();
      const scannable = Scannable.fromIterable(table.schema, table.batches);

      expect(scannable.schema).toBe(table.schema);
      expect(scannable.numRows).toBeNull();
      expect(scannable.rescannable).toBe(false);
    });

    test("accepts an async iterable of batches", () => {
      const table = makeTable();
      async function* generator(): AsyncGenerator<RecordBatch> {
        for (const batch of table.batches) {
          yield batch;
        }
      }

      const scannable = Scannable.fromIterable(table.schema, generator());
      expect(scannable.schema).toBe(table.schema);
      expect(scannable.rescannable).toBe(false);
    });

    test("honors rescannable override", () => {
      const table = makeTable();
      const scannable = Scannable.fromIterable(table.schema, table.batches, {
        rescannable: true,
      });
      expect(scannable.rescannable).toBe(true);
    });
  });

  describe("fromFactory", () => {
    test("defaults rescannable=true and does not invoke the factory eagerly", () => {
      const table = makeTable();
      const factory = jest.fn(() => table.batches);

      const scannable = Scannable.fromFactory(table.schema, factory);

      expect(scannable.schema).toBe(table.schema);
      expect(scannable.rescannable).toBe(true);
      expect(factory).not.toHaveBeenCalled();
    });

    test("honors rescannable and numRows overrides", () => {
      const table = makeTable();
      const scannable = Scannable.fromFactory(
        table.schema,
        () => table.batches,
        { numRows: 7, rescannable: false },
      );

      expect(scannable.numRows).toBe(7);
      expect(scannable.rescannable).toBe(false);
    });
  });

  describe("validation", () => {
    test("throws when numRows is negative", () => {
      expect(() => Scannable.fromTable(makeTable(), { numRows: -1 })).toThrow(
        /non-negative/,
      );
    });
  });

  describe("native handle", () => {
    test("exposes a native handle via inner", () => {
      const scannable = Scannable.fromTable(makeTable());
      expect(scannable.inner).toBeDefined();
      expect(typeof scannable.inner).toBe("object");
      expect(scannable.inner).not.toBeNull();
    });
  });

  // Schema-variety construction tests. Each asserts that construction
  // succeeds against a richer Arrow schema, which transitively exercises
  // `encodeSchema` and the Rust-side `ipc_file_to_schema` for types beyond
  // flat primitives.
  describe("schema variety", () => {
    test("accepts an empty table", () => {
      const schema = new Schema([new Field("id", new Int32(), true)]);
      const table = makeEmptyTable(schema);
      const scannable = Scannable.fromTable(table);

      expect(scannable.numRows).toBe(0);
      expect(scannable.schema).toBe(table.schema);
    });

    test("accepts nested struct and list columns", () => {
      const table = makeArrowTable(
        [
          { id: 1, point: { x: 0, y: 0 }, tags: ["a", "b"] },
          { id: 2, point: { x: 1, y: 2 }, tags: ["c"] },
        ],
        { vectorColumns: {} },
      );
      const scannable = Scannable.fromTable(table);

      expect(scannable.schema).toBe(table.schema);
      expect(scannable.numRows).toBe(2);
    });

    test("accepts a FixedSizeList (vector) column", () => {
      const table = makeArrowTable(
        [
          { id: 1, vec: [1, 2, 3] },
          { id: 2, vec: [4, 5, 6] },
        ],
        { vectorColumns: { vec: { type: new Float16() } } },
      );
      const scannable = Scannable.fromTable(table);

      expect(scannable.schema).toBe(table.schema);
      expect(scannable.numRows).toBe(2);
    });

    test("accepts a table with many columns", () => {
      const row: Record<string, number> = {};
      for (let i = 0; i < 50; i++) row[`c${i}`] = i;
      const table = makeArrowTable([row, row], { vectorColumns: {} });
      const scannable = Scannable.fromTable(table);

      expect(scannable.schema.fields.length).toBe(50);
      expect(scannable.numRows).toBe(2);
    });
  });
});
