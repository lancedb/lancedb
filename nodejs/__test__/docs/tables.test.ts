import * as arrow from "apache-arrow";
import { Field, FixedSizeList, Float16, Int32, Schema } from "apache-arrow";
import * as lancedb from "../../";

test("create_f16_table", async () => {
  // --8<-- [start:create_f16_table]
  const db = await lancedb.connect("/tmp/db");
  const dim = 16;
  const total = 10;
  const f16Schema = new Schema([
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
    { schema: f16Schema },
  );
  const table = await db.createTable("f16_tbl", data);
  table.update({ vector: [10, 10] }, { where: "id = 1" });
  // --8<-- [end:create_f16_table]
  expect(table).toBeDefined();
});
