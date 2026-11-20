// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import { decodePrewarmDataPlan } from "../lancedb/prewarm";
import { Table } from "../lancedb/table";

test.each([null, 0, 2])(
  "preview preserves unknown and zero range counts: %s",
  (ranges) => {
    const plan = decodePrewarmDataPlan(
      JSON.parse(`{
    "table_version":42,"plan":"display text",
    "pruning":{"mode":"future_mode","input_fragments":3,"selected_fragments":0,
      "pruned_fragments":3,"selected_row_ranges":${ranges}}
  }`),
    );
    expect(plan.tableVersion).toBe(42);
    expect(plan.pruning.mode).toBe("future_mode");
    expect(plan.pruning.selectedRowRanges).toBe(ranges);
    expect(plan.pruning.fallbackReason).toBeNull();
  },
);

test("custom tables inherit an unsupported preview default", async () => {
  // Only the inherited default is exercised; unrelated abstract operations
  // do not need implementations in this fixture.
  const table = Object.create(Table.prototype) as Table;
  await expect(
    table.explainPrewarmData({ columns: ["payload"], filter: "id > 0" }),
  ).rejects.toThrow(
    "data prewarm previews are not supported by this table implementation",
  );
});
