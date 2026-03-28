// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import * as tmp from "tmp";

import { type Table, connect } from "../lancedb";
import {
  Field,
  FixedSizeList,
  Float16,
  Float32,
  Float64,
  Int64,
  Schema,
  Uint8,
  makeArrowTable,
} from "../lancedb/arrow";

describe("Vector query with different typed arrays", () => {
  let tmpDir: tmp.DirResult;

  afterEach(() => {
    tmpDir?.removeCallback();
  });

  async function createFloat32Table(): Promise<Table> {
    tmpDir = tmp.dirSync({ unsafeCleanup: true });
    const db = await connect(tmpDir.name);
    const schema = new Schema([
      new Field("id", new Int64(), true),
      new Field(
        "vec",
        new FixedSizeList(2, new Field("item", new Float32())),
        true,
      ),
    ]);
    const data = makeArrowTable(
      [
        { id: 1n, vec: [1.0, 0.0] },
        { id: 2n, vec: [0.0, 1.0] },
        { id: 3n, vec: [1.0, 1.0] },
      ],
      { schema },
    );
    return db.createTable("test_f32", data);
  }

  it("should search with Float32Array (baseline)", async () => {
    const table = await createFloat32Table();
    const results = await table
      .query()
      .nearestTo(new Float32Array([1.0, 0.0]))
      .limit(1)
      .toArray();

    expect(results.length).toBe(1);
    expect(Number(results[0].id)).toBe(1);
  });

  it("should search with number[] (backward compat)", async () => {
    const table = await createFloat32Table();
    const results = await table
      .query()
      .nearestTo([1.0, 0.0])
      .limit(1)
      .toArray();

    expect(results.length).toBe(1);
    expect(Number(results[0].id)).toBe(1);
  });

  it("should search with Float64Array via raw path", async () => {
    const table = await createFloat32Table();
    const results = await table
      .query()
      .nearestTo(new Float64Array([1.0, 0.0]))
      .limit(1)
      .toArray();

    expect(results.length).toBe(1);
    expect(Number(results[0].id)).toBe(1);
  });

  it("should add multiple query vectors with Float64Array", async () => {
    const table = await createFloat32Table();
    const results = await table
      .query()
      .nearestTo(new Float64Array([1.0, 0.0]))
      .addQueryVector(new Float64Array([0.0, 1.0]))
      .limit(2)
      .toArray();

    expect(results.length).toBeGreaterThanOrEqual(2);
  });

  // Float16Array is only available in Node 22+
  const hasFloat16 = typeof globalThis.Float16Array !== "undefined";
  const f16it = hasFloat16 ? it : it.skip;

  f16it("should search with Float16Array via raw path", async () => {
    const table = await createFloat32Table();
    // @ts-expect-error Float16Array not in TS types yet
    const results = await table
      .query()
      // @ts-expect-error Float16Array not in TS types yet
      .nearestTo(new globalThis.Float16Array([1.0, 0.0]))
      .limit(1)
      .toArray();

    expect(results.length).toBe(1);
    expect(Number(results[0].id)).toBe(1);
  });
});
