// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import { Int32, Table, tableFromIPC, vectorFromArray } from "../lancedb/arrow";
import { toParameterBuffer } from "../lancedb/sql";

describe("SQL parameter encoding", () => {
  test("positional and named bindings preserve values without SQL interpolation", async () => {
    const attack = "training' OR TRUE; DROP TABLE items; --";
    const date = new Date("2026-01-02T00:00:00Z");
    const batch = tableFromIPC(
      await toParameterBuffer([
        42n,
        attack,
        null,
        true,
        date,
        0.7,
        new Uint8Array([1, 2]),
      ]),
    );
    expect(batch.schema.fields.map((field) => field.name)).toEqual([
      "1",
      "2",
      "3",
      "4",
      "5",
      "6",
      "7",
    ]);
    expect(batch.getChildAt(0)?.get(0)).toBe(42n);
    expect(batch.getChildAt(1)?.get(0)).toBe(attack);
    expect(batch.getChildAt(2)?.get(0)).toBeNull();
    expect(batch.getChildAt(4)?.get(0)).toBe(date.getTime());
    const named = tableFromIPC(
      await toParameterBuffer({ minimum: 42n, tag: attack }),
    );
    expect(named.schema.fields.map((field) => field.name)).toEqual([
      "minimum",
      "tag",
    ]);
    expect(named.getChild("tag")?.get(0)).toBe(attack);
  });

  test("explicit Arrow input preserves its types", async () => {
    const input = new Table({ minimum: vectorFromArray([42], new Int32()) });
    for (const parameters of [input, input.batches[0]]) {
      const decoded = tableFromIPC(await toParameterBuffer(parameters));
      expect(decoded.schema.fields[0].type.toString()).toBe("Int32");
      expect(decoded.getChildAt(0)?.get(0)).toBe(42);
    }
  });

  test.each([
    [],
    { value: undefined },
    { value: 2 ** 53 },
    { value: 1n << 63n },
    { value: {} },
    { value: new Date("invalid") },
  ])("invalid parameters fail before submission: %p", async (parameters) => {
    await expect(toParameterBuffer(parameters as never)).rejects.toThrow();
  });
});
