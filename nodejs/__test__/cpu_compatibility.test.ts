// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import * as tmp from "tmp";

import { connect } from "../lancedb";
import {
  Field,
  FixedSizeList,
  Float32,
  Int32,
  Schema,
  makeArrowTable,
} from "../lancedb/arrow";

test("cosine vector search runs on the pre-Haswell build baseline", async () => {
  const tmpDir = tmp.dirSync({ unsafeCleanup: true });

  try {
    const db = await connect(tmpDir.name);
    const schema = new Schema([
      new Field("id", new Int32(), false),
      new Field(
        "vector",
        new FixedSizeList(3, new Field("item", new Float32(), false)),
        false,
      ),
    ]);
    const data = makeArrowTable(
      [
        { id: 1, vector: [1, 0, 0] },
        { id: 2, vector: [0, 1, 0] },
      ],
      { schema },
    );
    const table = await db.createTable("vectors", data);

    const results = await table
      .vectorSearch([1, 0, 0])
      .distanceType("cosine")
      .limit(1)
      .toArray();

    expect(results).toHaveLength(1);
    expect(results[0].id).toBe(1);
    expect(results[0]._distance).toBeCloseTo(0);
  } finally {
    tmpDir.removeCallback();
  }
}, 60_000);
