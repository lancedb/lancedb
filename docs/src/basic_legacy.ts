// --8<-- [start:import]
import * as lancedb from "vectordb";
import {
  Schema,
  Field,
  Float32,
  FixedSizeList,
  Int32,
  Float16,
} from "apache-arrow";
import * as arrow from "apache-arrow";
// --8<-- [end:import]
import * as fs from "fs";
import { Table as ArrowTable, Utf8 } from "apache-arrow";

const example = async () => {
  fs.rmSync("data/sample-lancedb", { recursive: true, force: true });
  // --8<-- [start:open_db]
  const lancedb = require("vectordb");
  const uri = "data/sample-lancedb";
  const db = await lancedb.connect(uri);
  // --8<-- [end:open_db]

  // --8<-- [start:create_table]
  const tbl = await db.createTable(
    "myTable",
    [
      { vector: [3.1, 4.1], item: "foo", price: 10.0 },
      { vector: [5.9, 26.5], item: "bar", price: 20.0 },
    ],
    { writeMode: lancedb.WriteMode.Overwrite },
  );
  // --8<-- [end:create_table]
  {
    // --8<-- [start:create_table_with_schema]
    const schema = new arrow.Schema([
      new arrow.Field(
        "vector",
        new arrow.FixedSizeList(
          2,
          new arrow.Field("item", new arrow.Float32(), true),
        ),
      ),
      new arrow.Field("item", new arrow.Utf8(), true),
      new arrow.Field("price", new arrow.Float32(), true),
    ]);
    const data = [
      { vector: [3.1, 4.1], item: "foo", price: 10.0 },
      { vector: [5.9, 26.5], item: "bar", price: 20.0 },
    ];
    const tbl = await db.createTable({
      name: "myTableWithSchema",
      data,
      schema,
    });
    // --8<-- [end:create_table_with_schema]
  }

  // --8<-- [start:add]
  const newData = Array.from({ length: 500 }, (_, i) => ({
    vector: [i, i + 1],
    item: "fizz",
    price: i * 0.1,
  }));
  await tbl.add(newData);
  // --8<-- [end:add]

  // --8<-- [start:create_index]
  await tbl.createIndex({
    type: "ivf_pq",
    num_partitions: 2,
    num_sub_vectors: 2,
  });
  // --8<-- [end:create_index]

  // --8<-- [start:create_empty_table]
  const schema = new arrow.Schema([
    new arrow.Field("id", new arrow.Int32()),
    new arrow.Field("name", new arrow.Utf8()),
  ]);

  const empty_tbl = await db.createTable({ name: "empty_table", schema });
  // --8<-- [end:create_empty_table]
  {
    // --8<-- [start:create_f16_table]
    const dim = 16;
    const total = 10;
    const schema = new Schema([
      new Field("id", new Int32()),
      new Field(
        "vector",
        new FixedSizeList(dim, new Field("item", new Float16(), true)),
        false,
      ),
    ]);
    const data = lancedb.makeArrowTable(
      Array.from(Array(total), (_, i) => ({
        id: i,
        vector: Array.from(Array(dim), Math.random),
      })),
      { schema },
    );
    const table = await db.createTable("f16_tbl", data);
    // --8<-- [end:create_f16_table]
  }

  // --8<-- [start:search]
  const query = await tbl.search([100, 100]).limit(2).execute();
  // --8<-- [end:search]
  console.log(query);

  // --8<-- [start:delete]
  await tbl.delete('item = "fizz"');
  // --8<-- [end:delete]

  // --8<-- [start:drop_table]
  await db.dropTable("myTable");
  // --8<-- [end:drop_table]
};

async function main() {
  await example();
  console.log("Basic example: done");
}

main();
