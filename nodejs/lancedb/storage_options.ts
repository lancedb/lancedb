// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

/**
 * Takes storage options and makes all the keys snake case.
 *
 * @internal
 */
export function cleanseStorageOptions(
  options?: Record<string, string>,
): Record<string, string> | undefined {
  if (options === undefined) {
    return undefined;
  }
  const result: Record<string, string> = {};
  for (const [key, value] of Object.entries(options)) {
    if (value !== undefined) {
      const newKey = camelToSnakeCase(key);
      result[newKey] = value;
    }
  }
  return result;
}

/**
 * Convert a string to snake case. It might already be snake case, in which case it is
 * returned unchanged.
 */
function camelToSnakeCase(camel: string): string {
  if (camel.includes("_")) {
    // Assume if there is at least one underscore, it is already snake case
    return camel;
  }
  if (camel.toLocaleUpperCase() === camel) {
    // Assume if the string is all uppercase, it is already snake case
    return camel;
  }

  let result = camel.replace(/[A-Z]/g, (letter) => `_${letter.toLowerCase()}`);
  if (result.startsWith("_")) {
    result = result.slice(1);
  }
  return result;
}
