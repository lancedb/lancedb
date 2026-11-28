// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import { type BloomFilterOptions, Index } from "../lancedb";

describe("BloomFilter options", () => {
  it.each([
    0,
    -1,
    1.5,
    Number.NaN,
    Number.POSITIVE_INFINITY,
    Number.NEGATIVE_INFINITY,
    Number.MAX_SAFE_INTEGER + 1,
  ])("rejects invalid numberOfItems %s", (numberOfItems) => {
    expect(() => Index.bloomFilter({ numberOfItems })).toThrow(
      "numberOfItems must be a positive safe integer",
    );
  });

  it.each([
    -0.1,
    0,
    1,
    1.1,
    Number.NaN,
    Number.POSITIVE_INFINITY,
    Number.NEGATIVE_INFINITY,
  ])("rejects invalid probability %s", (probability) => {
    expect(() => Index.bloomFilter({ probability })).toThrow(
      "probability must be finite and strictly between zero and one",
    );
  });

  it.each<BloomFilterOptions>([
    {},
    { numberOfItems: 1 },
    { numberOfItems: Number.MAX_SAFE_INTEGER },
    { probability: Number.MIN_VALUE },
    { probability: 1 - Number.EPSILON },
  ])("accepts valid options %j", (options) => {
    expect(() => Index.bloomFilter(options)).not.toThrow();
  });
});
