// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import { Bool, Field, Int32, List, Schema, Struct, Utf8 } from "apache-arrow";

import * as arrow15 from "apache-arrow-15";
import * as arrow16 from "apache-arrow-16";
import * as arrow17 from "apache-arrow-17";
import * as arrow18 from "apache-arrow-18";

import {
  convertToTable,
  fromBufferToRecordBatch,
  fromRecordBatchToBuffer,
  fromTableToBuffer,
  makeArrowTable,
  makeEmptyTable,
  tableFromIPC,
} from "../lancedb/arrow";
import {
  EmbeddingFunction,
  FieldOptions,
  FunctionOptions,
} from "../lancedb/embedding/embedding_function";
import { EmbeddingFunctionConfig } from "../lancedb/embedding/registry";

// biome-ignore lint/suspicious/noExplicitAny: skip
function sampleRecords(): Array<Record<string, any>> {
  return [
    {
      binary: Buffer.alloc(5),
      boolean: false,
      number: 7,
      string: "hello",
      struct: { x: 0, y: 0 },
      list: ["anime", "action", "comedy"],
    },
  ];
}
describe.each([arrow15, arrow16, arrow17, arrow18])(
  "Arrow",
  (
    arrow: typeof arrow15 | typeof arrow16 | typeof arrow17 | typeof arrow18,
  ) => {
    type ApacheArrow =
      | typeof arrow15
      | typeof arrow16
      | typeof arrow17
      | typeof arrow18;
    const {
      Schema,
      Field,
      Binary,
      Bool,
      Utf8,
      Float64,
      Struct,
      List,
      Int16,
      Int32,
      Int64,
      Float,
      Float16,
      Float32,
      FixedSizeList,
      Precision,
      tableFromIPC,
      DataType,
      Dictionary,
      // biome-ignore lint/suspicious/noExplicitAny: <explanation>
    } = <any>arrow;
    type Schema = ApacheArrow["Schema"];
    type Table = ApacheArrow["Table"];

    // Helper method to verify various ways to create a table
    async function checkTableCreation(
      tableCreationMethod: (
        records: Record<string, unknown>[],
        recordsReversed: Record<string, unknown>[],
        schema: Schema,
      ) => Promise<Table>,
      infersTypes: boolean,
    ): Promise<void> {
      const records = sampleRecords();
      const recordsReversed = [
        {
          list: ["anime", "action", "comedy"],
          struct: { x: 0, y: 0 },
          string: "hello",
          number: 7,
          boolean: false,
          binary: Buffer.alloc(5),
        },
      ];
      const schema = new Schema([
        new Field("binary", new Binary(), false),
        new Field("boolean", new Bool(), false),
        new Field("number", new Float64(), false),
        new Field("string", new Utf8(), false),
        new Field(
          "struct",
          new Struct([
            new Field("x", new Float64(), false),
            new Field("y", new Float64(), false),
          ]),
        ),
        new Field(
          "list",
          new List(new Field("item", new Utf8(), false)),
          false,
        ),
      ]);
      const table = (await tableCreationMethod(
        records,
        recordsReversed,
        schema,
        // biome-ignore lint/suspicious/noExplicitAny: <explanation>
      )) as any;

      // We expect deterministic ordering of the fields
      expect(table.schema.names).toEqual(schema.names);

      schema.fields.forEach(
        (
          // biome-ignore lint/suspicious/noExplicitAny: <explanation>
          field: { name: any; type: { toString: () => any } },
          idx: string | number,
        ) => {
          const actualField = table.schema.fields[idx];
          // Type inference always assumes nullable=true
          if (infersTypes) {
            expect(actualField.nullable).toBe(true);
          } else {
            expect(actualField.nullable).toBe(false);
          }
          expect(table.getChild(field.name)?.type.toString()).toEqual(
            field.type.toString(),
          );
          expect(table.getChildAt(idx)?.type.toString()).toEqual(
            field.type.toString(),
          );
        },
      );
    }

    describe("The function makeArrowTable", function () {
      it("will use data types from a provided schema instead of inference", async function () {
        const schema = new Schema([
          new Field("a", new Int32(), false),
          new Field("b", new Float32(), true),
          new Field(
            "c",
            new FixedSizeList(3, new Field("item", new Float16())),
          ),
          new Field("d", new Int64(), true),
        ]);
        const table = makeArrowTable(
          [
            { a: 1, b: 2, c: [1, 2, 3], d: 9 },
            { a: 4, b: 5, c: [4, 5, 6], d: 10 },
            { a: 7, b: 8, c: [7, 8, 9], d: null },
          ],
          { schema },
        );

        const buf = await fromTableToBuffer(table);
        expect(buf.byteLength).toBeGreaterThan(0);

        const actual = tableFromIPC(buf);
        expect(actual.numRows).toBe(3);
        const actualSchema = actual.schema;
        expect(actualSchema).toEqual(schema);
        expect(table.getChild("a")?.toJSON()).toEqual([1, 4, 7]);
        expect(table.getChild("b")?.toJSON()).toEqual([2, 5, 8]);
        expect(table.getChild("d")?.toJSON()).toEqual([9n, 10n, null]);
      });

      it("will assume the column `vector` is FixedSizeList<Float32> by default", async function () {
        const schema = new Schema([
          new Field("a", new Float(Precision.DOUBLE), true),
          new Field("b", new Int64(), true),
          new Field(
            "vector",
            new FixedSizeList(
              3,
              new Field("item", new Float(Precision.SINGLE), true),
            ),
            true,
          ),
        ]);
        const table = makeArrowTable([
          { a: 1, b: 2n, vector: [1, 2, 3] },
          { a: 4, b: 5n, vector: [4, 5, 6] },
          { a: 7, b: 8n, vector: [7, 8, 9] },
        ]);

        const buf = await fromTableToBuffer(table);
        expect(buf.byteLength).toBeGreaterThan(0);

        const actual = tableFromIPC(buf);
        expect(actual.numRows).toBe(3);
        const actualSchema = actual.schema;
        expect(actualSchema).toEqual(schema);

        expect(table.getChild("a")?.toJSON()).toEqual([1, 4, 7]);
        expect(table.getChild("b")?.toJSON()).toEqual([2n, 5n, 8n]);
        expect(
          table
            .getChild("vector")
            ?.toJSON()
            .map((v) => v.toJSON()),
        ).toEqual([
          [1, 2, 3],
          [4, 5, 6],
          [7, 8, 9],
        ]);
      });

      it("can support multiple vector columns", async function () {
        const schema = new Schema([
          new Field("a", new Float(Precision.DOUBLE), true),
          new Field("b", new Float(Precision.DOUBLE), true),
          new Field(
            "vec1",
            new FixedSizeList(3, new Field("item", new Float16(), true)),
            true,
          ),
          new Field(
            "vec2",
            new FixedSizeList(3, new Field("item", new Float64(), true)),
            true,
          ),
        ]);
        const table = makeArrowTable(
          [
            { a: 1, b: 2, vec1: [1, 2, 3], vec2: [2, 4, 6] },
            { a: 4, b: 5, vec1: [4, 5, 6], vec2: [8, 10, 12] },
            { a: 7, b: 8, vec1: [7, 8, 9], vec2: [14, 16, 18] },
          ],
          {
            vectorColumns: {
              vec1: { type: new Float16() },
              vec2: { type: new Float64() },
            },
          },
        );

        const buf = await fromTableToBuffer(table);
        expect(buf.byteLength).toBeGreaterThan(0);

        const actual = tableFromIPC(buf);
        expect(actual.numRows).toBe(3);
        const actualSchema = actual.schema;
        expect(actualSchema).toEqual(schema);
      });

      it("will allow different vector column types", async function () {
        const table = makeArrowTable([{ fp16: [1], fp32: [1], fp64: [1] }], {
          vectorColumns: {
            fp16: { type: new Float16() },
            fp32: { type: new Float32() },
            fp64: { type: new Float64() },
          },
        });

        expect(
          table.getChild("fp16")?.type.children[0].type.toString(),
        ).toEqual(new Float16().toString());
        expect(
          table.getChild("fp32")?.type.children[0].type.toString(),
        ).toEqual(new Float32().toString());
        expect(
          table.getChild("fp64")?.type.children[0].type.toString(),
        ).toEqual(new Float64().toString());
      });

      it("will use dictionary encoded strings if asked", async function () {
        const table = makeArrowTable([{ str: "hello" }]);
        expect(DataType.isUtf8(table.getChild("str")?.type)).toBe(true);

        const tableWithDict = makeArrowTable([{ str: "hello" }], {
          dictionaryEncodeStrings: true,
        });
        expect(DataType.isDictionary(tableWithDict.getChild("str")?.type)).toBe(
          true,
        );

        const schema = new Schema([
          new Field("str", new Dictionary(new Utf8(), new Int32())),
        ]);

        const tableWithDict2 = makeArrowTable([{ str: "hello" }], { schema });
        expect(
          DataType.isDictionary(tableWithDict2.getChild("str")?.type),
        ).toBe(true);
      });

      it("will infer data types correctly", async function () {
        await checkTableCreation(
          // biome-ignore lint/suspicious/noExplicitAny: <explanation>
          async (records) => (<any>makeArrowTable)(records),
          true,
        );
      });

      it("will allow a schema to be provided", async function () {
        await checkTableCreation(
          async (records, _, schema) =>
            // biome-ignore lint/suspicious/noExplicitAny: <explanation>
            (<any>makeArrowTable)(records, { schema }),
          false,
        );
      });

      it("will use the field order of any provided schema", async function () {
        await checkTableCreation(
          async (_, recordsReversed, schema) =>
            // biome-ignore lint/suspicious/noExplicitAny: <explanation>
            (<any>makeArrowTable)(recordsReversed, { schema }),
          false,
        );
      });

      it("will make an empty table", async function () {
        await checkTableCreation(
          // biome-ignore lint/suspicious/noExplicitAny: <explanation>
          async (_, __, schema) => (<any>makeArrowTable)([], { schema }),
          false,
        );
      });

      it("will allow subsets of columns if nullable", async function () {
        const schema = new Schema([
          new Field("a", new Int64(), true),
          new Field(
            "s",
            new Struct([
              new Field("x", new Int32(), true),
              new Field("y", new Int32(), true),
            ]),
            true,
          ),
          new Field("d", new Int16(), true),
        ]);

        const table = makeArrowTable([{ a: 1n }], { schema });
        expect(table.numCols).toBe(1);
        expect(table.numRows).toBe(1);

        const table2 = makeArrowTable([{ a: 1n, d: 2 }], { schema });
        expect(table2.numCols).toBe(2);

        const table3 = makeArrowTable([{ s: { y: 3 } }], { schema });
        expect(table3.numCols).toBe(1);
        const expectedSchema = new Schema([
          new Field("s", new Struct([new Field("y", new Int32(), true)]), true),
        ]);
        expect(table3.schema).toEqual(expectedSchema);
      });

      it("will work even if columns are sparsely provided", async function () {
        const sparseRecords = [{ a: 1n }, { b: 2n }, { c: 3n }, { d: 4n }];
        const table = makeArrowTable(sparseRecords);
        expect(table.numCols).toBe(4);
        expect(table.numRows).toBe(4);

        const schema = new Schema([
          new Field("a", new Int64(), true),
          new Field("b", new Int32(), true),
          new Field("c", new Int64(), true),
          new Field("d", new Int16(), true),
        ]);
        const table2 = makeArrowTable(sparseRecords, { schema });
        expect(table2.numCols).toBe(4);
        expect(table2.numRows).toBe(4);
        expect(table2.schema).toEqual(schema);
      });

      it("will handle missing columns in schema alignment when using embeddings", async function () {
        // Create a schema with embedding metadata (like LanceSchema does)
        const schema = new Schema(
          [
            new Field("domain", new Utf8(), true),
            new Field("name", new Utf8(), true),
            new Field("description", new Utf8(), true),
          ],
          new Map([["embedding_functions", JSON.stringify([])]]),
        );

        // Data is missing the "description" field
        const data = [
          { domain: "google.com", name: "Google" },
          { domain: "facebook.com", name: "Facebook" },
        ];

        // This should NOT throw an error when using convertToTable with schema that has embedding metadata
        const table = await convertToTable(data, undefined, { schema });

        // Should create all 3 columns (including null column for description)
        expect(table.numCols).toBe(3);
        expect(table.numRows).toBe(2);

        // Check that the missing column was filled with nulls
        const descriptionColumn = table.getChild("description");
        expect(descriptionColumn).toBeDefined();
        expect(descriptionColumn?.nullCount).toBe(2); // All values are null
        expect(descriptionColumn?.toArray()).toEqual([null, null]);

        // Check that existing columns have correct values
        expect(table.getChild("domain")?.toArray()).toEqual([
          "google.com",
          "facebook.com",
        ]);
        expect(table.getChild("name")?.toArray()).toEqual([
          "Google",
          "Facebook",
        ]);
      });

      it("will handle completely missing nested struct columns", async function () {
        // Schema with nested struct
        const schema = new Schema(
          [
            new Field("id", new Utf8(), true),
            new Field("name", new Utf8(), true),
            new Field(
              "metadata",
              new Struct([
                new Field("version", new Int32(), true),
                new Field("author", new Utf8(), true),
                new Field(
                  "tags",
                  new List(new Field("item", new Utf8(), true)),
                  true,
                ),
              ]),
              true,
            ),
          ],
          new Map([["embedding_functions", JSON.stringify([])]]),
        );

        // Data completely missing the nested struct
        const data = [
          { id: "doc1", name: "Document 1" },
          { id: "doc2", name: "Document 2" },
        ];

        // This should NOT throw an error
        const table = await convertToTable(data, undefined, { schema });

        // Should create all columns including the nested struct
        expect(table.numCols).toBe(3);
        expect(table.numRows).toBe(2);

        // Convert to buffer and back (simulating storage and retrieval)
        const buf = await fromTableToBuffer(table);
        const retrievedTable = tableFromIPC(buf);

        // Verify the retrieved table has the same structure
        const rows = [];
        for (let i = 0; i < retrievedTable.numRows; i++) {
          rows.push(retrievedTable.get(i));
        }

        // Should have 2 rows
        expect(rows.length).toBe(2);

        // Check that the nested struct column was created with null values for all fields
        expect(rows[0].metadata.version).toBe(null);
        expect(rows[0].metadata.author).toBe(null);
        expect(rows[0].metadata.tags).toBe(null);
        expect(rows[1].metadata.version).toBe(null);
        expect(rows[1].metadata.author).toBe(null);
        expect(rows[1].metadata.tags).toBe(null);

        // Check that existing columns have correct values
        expect(rows[0].id).toBe("doc1");
        expect(rows[0].name).toBe("Document 1");
        expect(rows[1].id).toBe("doc2");
        expect(rows[1].name).toBe("Document 2");
      });

      it("will handle partially missing nested struct fields", async function () {
        // Schema with nested struct
        const schema = new Schema(
          [
            new Field("id", new Utf8(), true),
            new Field(
              "metadata",
              new Struct([
                new Field("version", new Int32(), true),
                new Field("author", new Utf8(), true),
                new Field("created_at", new Utf8(), true),
              ]),
              true,
            ),
          ],
          new Map([["embedding_functions", JSON.stringify([])]]),
        );

        // Data with partially missing nested fields
        const data = [
          { id: "doc1", metadata: { version: 1, author: "Alice" } }, // missing created_at
          { id: "doc2", metadata: { version: 2 } }, // missing author and created_at
        ];

        // This should NOT throw an error
        const table = await convertToTable(data, undefined, { schema });

        // Should create all columns
        expect(table.numCols).toBe(2);
        expect(table.numRows).toBe(2);

        // The core functionality should work - table creation should not throw
        expect(table.numCols).toBe(2);
        expect(table.numRows).toBe(2);

        // Check that the metadata column exists and has the expected schema
        const metadataColumn = table.getChild("metadata");
        expect(metadataColumn).toBeDefined();
        expect(metadataColumn?.type.toString()).toBe(
          "Struct<{version:Int32, author:Utf8, created_at:Utf8}>",
        );

        // Verify the table structure is correct
        expect(table.schema.fields.length).toBe(2);
        expect(table.schema.fields[0].name).toBe("id");
        expect(table.schema.fields[1].name).toBe("metadata");
      });

      it("will handle multiple levels of nested structures", async function () {
        // Schema with deeply nested struct
        const schema = new Schema(
          [
            new Field("id", new Utf8(), true),
            new Field(
              "config",
              new Struct([
                new Field("database", new Utf8(), true),
                new Field(
                  "connection",
                  new Struct([
                    new Field("host", new Utf8(), true),
                    new Field("port", new Int32(), true),
                    new Field(
                      "ssl",
                      new Struct([
                        new Field("enabled", new Bool(), true),
                        new Field("cert_path", new Utf8(), true),
                      ]),
                      true,
                    ),
                  ]),
                  true,
                ),
              ]),
              true,
            ),
          ],
          new Map([["embedding_functions", JSON.stringify([])]]),
        );

        // Data with various levels of missing nested fields
        const data = [
          {
            id: "config1",
            config: {
              database: "postgres",
              connection: {
                host: "localhost",
                // missing port and ssl
              },
            },
          },
          {
            id: "config2",
            config: {
              database: "mysql",
              // missing entire connection object
            },
          },
          {
            id: "config3",
            // missing entire config object
          },
        ];

        // This should NOT throw an error
        const table = await convertToTable(data, undefined, { schema });

        // Should create all columns
        expect(table.numCols).toBe(2);
        expect(table.numRows).toBe(3);

        // The core functionality should work - table creation should not throw
        expect(table.numCols).toBe(2);
        expect(table.numRows).toBe(3);

        // Check that the config column exists and has the expected schema
        const configColumn = table.getChild("config");
        expect(configColumn).toBeDefined();
        expect(configColumn?.type.toString()).toBe(
          "Struct<{database:Utf8, connection:Struct<{host:Utf8, port:Int32, ssl:Struct<{enabled:Bool, cert_path:Utf8}>}>}>",
        );

        // Verify the table structure is correct
        expect(table.schema.fields.length).toBe(2);
        expect(table.schema.fields[0].name).toBe("id");
        expect(table.schema.fields[1].name).toBe("config");
      });

      it("should correctly retain values in nested struct fields", async function () {
        // Define test data with nested struct
        const testData = [
          {
            id: "doc1",
            vector: [1, 2, 3],
            metadata: {
              filePath: "/path/to/file1.ts",
              startLine: 10,
              endLine: 20,
              text: "function test() { return true; }",
            },
          },
          {
            id: "doc2",
            vector: [4, 5, 6],
            metadata: {
              filePath: "/path/to/file2.ts",
              startLine: 30,
              endLine: 40,
              text: "function test2() { return false; }",
            },
          },
        ];

        // Create Arrow table from the data
        const table = makeArrowTable(testData);

        // Verify schema has the nested struct fields
        const metadataField = table.schema.fields.find(
          (f) => f.name === "metadata",
        );
        expect(metadataField).toBeDefined();
        // biome-ignore lint/suspicious/noExplicitAny: accessing fields in different Arrow versions
        const childNames = metadataField?.type.children.map((c: any) => c.name);
        expect(childNames).toEqual([
          "filePath",
          "startLine",
          "endLine",
          "text",
        ]);

        // Convert to buffer and back (simulating storage and retrieval)
        const buf = await fromTableToBuffer(table);
        const retrievedTable = tableFromIPC(buf);

        // Verify the retrieved table has the same structure
        const rows = [];
        for (let i = 0; i < retrievedTable.numRows; i++) {
          rows.push(retrievedTable.get(i));
        }

        // Check values in the first row
        const firstRow = rows[0];
        expect(firstRow.id).toBe("doc1");
        expect(firstRow.vector.toJSON()).toEqual([1, 2, 3]);

        // Verify metadata values are preserved (this is where the bug is)
        expect(firstRow.metadata).toBeDefined();
        expect(firstRow.metadata.filePath).toBe("/path/to/file1.ts");
        expect(firstRow.metadata.startLine).toBe(10);
        expect(firstRow.metadata.endLine).toBe(20);
        expect(firstRow.metadata.text).toBe("function test() { return true; }");
      });
    });

    class DummyEmbedding extends EmbeddingFunction<string> {
      toJSON(): Partial<FunctionOptions> {
        return {};
      }

      async computeSourceEmbeddings(data: string[]): Promise<number[][]> {
        return data.map(() => [0.0, 0.0]);
      }

      ndims(): number {
        return 2;
      }

      embeddingDataType() {
        return new Float16();
      }
    }

    class DummyEmbeddingWithNoDimension extends EmbeddingFunction<string> {
      toJSON(): Partial<FunctionOptions> {
        return {};
      }

      embeddingDataType() {
        return new Float16();
      }

      async computeSourceEmbeddings(data: string[]): Promise<number[][]> {
        return data.map(() => [0.0, 0.0]);
      }
    }
    const dummyEmbeddingConfig: EmbeddingFunctionConfig = {
      sourceColumn: "string",
      function: new DummyEmbedding(),
    };

    const dummyEmbeddingConfigWithNoDimension: EmbeddingFunctionConfig = {
      sourceColumn: "string",
      function: new DummyEmbeddingWithNoDimension(),
    };

    describe("convertToTable", function () {
      it("will infer data types correctly", async function () {
        await checkTableCreation(
          // biome-ignore lint/suspicious/noExplicitAny: <explanation>
          async (records) => await (<any>convertToTable)(records),
          true,
        );
      });

      it("will allow a schema to be provided", async function () {
        await checkTableCreation(
          async (records, _, schema) =>
            // biome-ignore lint/suspicious/noExplicitAny: <explanation>
            await (<any>convertToTable)(records, undefined, { schema }),
          false,
        );
      });

      it("will use the field order of any provided schema", async function () {
        await checkTableCreation(
          async (_, recordsReversed, schema) =>
            // biome-ignore lint/suspicious/noExplicitAny: <explanation>
            await (<any>convertToTable)(recordsReversed, undefined, { schema }),
          false,
        );
      });

      it("will make an empty table", async function () {
        await checkTableCreation(
          async (_, __, schema) =>
            // biome-ignore lint/suspicious/noExplicitAny: <explanation>
            await (<any>convertToTable)([], undefined, { schema }),
          false,
        );
      });

      it("will apply embeddings", async function () {
        const records = sampleRecords();
        const table = await convertToTable(records, dummyEmbeddingConfig);
        expect(DataType.isFixedSizeList(table.getChild("vector")?.type)).toBe(
          true,
        );
        expect(
          table.getChild("vector")?.type.children[0].type.toString(),
        ).toEqual(new Float16().toString());
      });

      it("will fail if missing the embedding source column", async function () {
        await expect(
          convertToTable([{ id: 1 }], dummyEmbeddingConfig),
        ).rejects.toThrow("'string' was not present");
      });

      it("use embeddingDimension if embedding missing from table", async function () {
        const schema = new Schema([new Field("string", new Utf8(), false)]);
        // Simulate getting an empty Arrow table (minus embedding) from some other source
        // In other words, we aren't starting with records
        const table = makeEmptyTable(schema);

        // If the embedding specifies the dimension we are fine
        await fromTableToBuffer(table, dummyEmbeddingConfig);

        // We can also supply a schema and should be ok
        const schemaWithEmbedding = new Schema([
          new Field("string", new Utf8(), false),
          new Field(
            "vector",
            new FixedSizeList(2, new Field("item", new Float16(), false)),
            false,
          ),
        ]);
        await fromTableToBuffer(
          table,
          dummyEmbeddingConfigWithNoDimension,
          schemaWithEmbedding,
        );

        // Otherwise we will get an error
        await expect(
          fromTableToBuffer(table, dummyEmbeddingConfigWithNoDimension),
        ).rejects.toThrow("does not specify `embeddingDimension`");
      });

      it("will apply embeddings to an empty table", async function () {
        const schema = new Schema([
          new Field("string", new Utf8(), false),
          new Field(
            "vector",
            new FixedSizeList(2, new Field("item", new Float16(), false)),
            false,
          ),
        ]);
        const table = await convertToTable([], dummyEmbeddingConfig, {
          schema,
        });
        expect(DataType.isFixedSizeList(table.getChild("vector")?.type)).toBe(
          true,
        );
        expect(
          table.getChild("vector")?.type.children[0].type.toString(),
        ).toEqual(new Float16().toString());
      });

      it("will complain if embeddings present but schema missing embedding column", async function () {
        const schema = new Schema([new Field("string", new Utf8(), false)]);
        await expect(
          convertToTable([], dummyEmbeddingConfig, { schema }),
        ).rejects.toThrow("column vector was missing");
      });

      it("will skip embedding application if already applied", async function () {
        const records = sampleRecords();
        const table = await convertToTable(records, dummyEmbeddingConfig);

        // fromTableToBuffer will try and apply the embeddings again
        // but should skip since the column already has non-null values
        const result = await fromTableToBuffer(table, dummyEmbeddingConfig);
        expect(result.byteLength).toBeGreaterThan(0);
      });
    });

    describe("makeEmptyTable", function () {
      it("will make an empty table", async function () {
        await checkTableCreation(
          // biome-ignore lint/suspicious/noExplicitAny: <explanation>
          async (_, __, schema) => (<any>makeEmptyTable)(schema),
          false,
        );
      });
    });

    describe("when using two versions of arrow", function () {
      it("can still import data", async function () {
        const schema = new arrow15.Schema([
          new arrow15.Field("id", new arrow15.Int32()),
          new arrow15.Field(
            "vector",
            new arrow15.FixedSizeList(
              1024,
              new arrow15.Field("item", new arrow15.Float32(), true),
            ),
          ),
          new arrow15.Field(
            "struct",
            new arrow15.Struct([
              new arrow15.Field(
                "nested",
                new arrow15.Dictionary(
                  new arrow15.Utf8(),
                  new arrow15.Int32(),
                  1,
                  true,
                ),
              ),
              new arrow15.Field(
                "ts_with_tz",
                new arrow15.TimestampNanosecond("some_tz"),
              ),
              new arrow15.Field(
                "ts_no_tz",
                new arrow15.TimestampNanosecond(null),
              ),
            ]),
          ),
          // biome-ignore lint/suspicious/noExplicitAny: skip
        ]) as any;
        schema.metadataVersion = arrow15.MetadataVersion.V5;
        const table = makeArrowTable([], { schema });

        const buf = await fromTableToBuffer(table);
        expect(buf.byteLength).toBeGreaterThan(0);
        const actual = tableFromIPC(buf);
        const actualSchema = actual.schema;
        expect(actualSchema.fields.length).toBe(3);

        // Deep equality gets hung up on some very minor unimportant differences
        // between arrow version 13 and 15 which isn't really what we're testing for
        // and so we do our own comparison that just checks name/type/nullability
        function compareFields(lhs: arrow15.Field, rhs: arrow15.Field) {
          expect(lhs.name).toEqual(rhs.name);
          expect(lhs.nullable).toEqual(rhs.nullable);
          expect(lhs.typeId).toEqual(rhs.typeId);
          if ("children" in lhs.type && lhs.type.children !== null) {
            const lhsChildren = lhs.type.children as arrow15.Field[];
            lhsChildren.forEach((child: arrow15.Field, idx) => {
              compareFields(child, rhs.type.children[idx]);
            });
          }
        }
        // biome-ignore lint/suspicious/noExplicitAny: <explanation>
        actualSchema.fields.forEach((field: any, idx: string | number) => {
          compareFields(field, actualSchema.fields[idx]);
        });
      });
    });

    describe("converting record batches to buffers", function () {
      it("can convert to buffered record batch and back again", async function () {
        const records = [
          { text: "dog", vector: [0.1, 0.2] },
          { text: "cat", vector: [0.3, 0.4] },
        ];
        const table = await convertToTable(records);
        const batch = table.batches[0];

        const buffer = await fromRecordBatchToBuffer(batch);
        const result = await fromBufferToRecordBatch(buffer);

        expect(JSON.stringify(batch.toArray())).toEqual(
          JSON.stringify(result?.toArray()),
        );
      });

      it("converting from buffer returns null if buffer has no record batches", async function () {
        const result = await fromBufferToRecordBatch(Buffer.from([0x01, 0x02])); // bad data
        expect(result).toEqual(null);
      });
    });
  },
);
