// --8<-- [start:import]
import * as lancedb from "vectordb";
import { Schema, Field, Float32, FixedSizeList, Int32 } from "apache-arrow";
// --8<-- [end:import]
import { Table as ArrowTable, Utf8 } from "apache-arrow";

const example = async () => {
  // --8<-- [start:open_db]
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
    { writeMode: lancedb.WriteMode.Overwrite }
  );
  // --8<-- [end:create_table]

  // --8<-- [start:create_empty_table]
  const schema = new Schema([
    new Field("id", new Int32()),
    new Field("name", new Utf8()),
  ]);
  const empty_tbl = await db.createTable({ name: "empty_table", schema });
  // --8<-- [end:create_empty_table]
};

await example();
