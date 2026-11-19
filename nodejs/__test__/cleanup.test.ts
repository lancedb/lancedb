// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import {
  type CleanupOptions,
  decodeCleanupPlan,
  decodeCleanupResult,
  validateCleanupOptions,
} from "../lancedb/cleanup";

test("cleanup never silently discards an unknown option", () => {
  expect(() =>
    validateCleanupOptions({ force: true } as unknown as CleanupOptions),
  ).toThrow("Unknown cleanup option");
});

test.each([undefined, 0, 604800])(
  "cleanup retention accepts omission and explicit zero (%s)",
  (age) => {
    expect(() => validateCleanupOptions({ minAgeSeconds: age })).not.toThrow();
  },
);

test.each([
  -1,
  1.5,
  Number.NaN,
  Number.POSITIVE_INFINITY,
  Number.MAX_SAFE_INTEGER + 1,
])("cleanup retention rejects invalid values (%s)", (age) => {
  expect(() => validateCleanupOptions({ minAgeSeconds: age })).toThrow();
});

test.each([0, 2, null])(
  "cleanup results distinguish known zero and unknown (%s)",
  (count) => {
    const result = decodeCleanupResult(
      JSON.parse(
        JSON.stringify({
          ["format_version"]: 1,
          ["min_age_seconds"]: 0,
          ["statistics_complete"]: count !== null,
          ["old_versions"]: count,
          ["bytes_removed"]: count,
          future: "ignored",
        }),
      ),
    );
    expect(result.oldVersions).toBe(count);
  },
);

test.each([
  null,
  {},
  { ["format_version"]: 2 },
  {
    ["format_version"]: 1,
    ["min_age_seconds"]: 0,
    ["statistics_complete"]: false,
    ["old_versions"]: 1,
  },
])("cleanup rejects incompatible results (%p)", (value) => {
  expect(() => decodeCleanupResult(value)).toThrow();
});

test("cleanup preview validates its versioned contract", () => {
  const value = JSON.parse(
    '{"format_version":1,"source_version":42,"min_age_seconds":0,"cutoff_millis":-1,"versions_to_remove":2,"bytes_to_remove":100}',
  );
  expect(decodeCleanupPlan(value).versionsToRemove).toBe(2);
  value.format_version = 2;
  expect(() => decodeCleanupPlan(value)).toThrow();
});
