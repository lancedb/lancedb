// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

const {
  checkNativeBinary,
  findForbiddenUndefinedSymbols,
} = require("../scripts/check-native-symbols.js");

test("detects the unresolved AVX-512 symbol from broken musl binaries", () => {
  const output = [
    "                 U napi_create_function",
    "                 U sum_4bit_dist_table_32bytes_batch_avx512",
    "                 U strlen",
  ].join("\n");

  expect(findForbiddenUndefinedSymbols(output)).toEqual([
    "sum_4bit_dist_table_32bytes_batch_avx512",
  ]);
});

test("accepts native binaries without unresolved internal Lance symbols", () => {
  const runNm = jest.fn(() => ({
    status: 0,
    stdout:
      "                 U napi_create_function\n                 U strlen\n",
    stderr: "",
  }));

  expect(() => checkNativeBinary("lancedb.node", runNm)).not.toThrow();
  expect(runNm).toHaveBeenCalledWith(
    "nm",
    ["-D", "--undefined-only", "lancedb.node"],
    { encoding: "utf8" },
  );
});

test("rejects native binaries with the unresolved AVX-512 symbol", () => {
  const runNm = jest.fn(() => ({
    status: 0,
    stdout: "                 U sum_4bit_dist_table_32bytes_batch_avx512\n",
    stderr: "",
  }));

  expect(() => checkNativeBinary("lancedb.node", runNm)).toThrow(
    "lancedb.node contains unresolved internal Lance symbols: sum_4bit_dist_table_32bytes_batch_avx512",
  );
});
