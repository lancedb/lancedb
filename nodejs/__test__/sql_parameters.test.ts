// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import { Int32, Table, tableFromIPC, vectorFromArray } from "../lancedb/arrow";
import { toParameterBuffer } from "../lancedb/sql";
import { LocalTable } from "../lancedb/table";

describe("SQL parameter encoding", () => {
  test.each([false, true])(
    "prewarm submission and preview forward typed bindings (named: %s)",
    async (named) => {
      const attack = "training' OR TRUE; DROP TABLE items; --";
      const parameters = named ? { minimum: 42n, tag: attack } : [42n, attack];
      const filter = named
        ? "id >= $minimum AND tag = $tag"
        : "id >= $1 AND tag = $2";
      const submission = jest.fn().mockResolvedValue({});
      const preview = jest.fn().mockResolvedValue(
        JSON.parse(`{
        "table_version": 42,
        "plan": "metadata preview",
        "pruning": {
          "mode": "zonemap_ranges",
          "input_fragments": 3,
          "selected_fragments": 1,
          "pruned_fragments": 2,
          "selected_row_ranges": 2
        }
      }`),
      );
      const table = new LocalTable({
        prewarmDataAsync: submission,
        explainPrewarmData: preview,
      } as never);
      await table.prewarmDataAsync({ filter, parameters });
      const plan = await table.explainPrewarmData({ filter, parameters });
      expect(plan.tableVersion).toBe(42);
      expect(plan.pruning.selectedFragments).toBe(1);
      for (const call of [submission, preview]) {
        expect(call).toHaveBeenCalledTimes(1);
        const options = call.mock.calls[0][0];
        expect(options.filter).toBe(filter);
        expect(options.filter).not.toContain(attack);
        expect(Buffer.isBuffer(options.parameters)).toBe(true);
        const batch = tableFromIPC(options.parameters);
        expect(batch.schema.fields.map((field) => field.name)).toEqual(
          named ? ["minimum", "tag"] : ["1", "2"],
        );
        expect(batch.getChildAt(0)?.get(0)).toBe(42n);
        expect(batch.getChildAt(1)?.get(0)).toBe(attack);
      }
    },
  );

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
