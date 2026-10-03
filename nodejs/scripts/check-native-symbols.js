// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

const { spawnSync } = require("node:child_process");
const { existsSync } = require("node:fs");
const path = require("node:path");

const FORBIDDEN_UNDEFINED_SYMBOLS = new Set([
  "sum_4bit_dist_table_32bytes_batch_avx512",
]);

function findForbiddenUndefinedSymbols(output) {
  const found = new Set();

  for (const line of output.split(/\r?\n/)) {
    const columns = line.trim().split(/\s+/);
    const symbol = columns.at(-1)?.split("@")[0];
    if (symbol && FORBIDDEN_UNDEFINED_SYMBOLS.has(symbol)) {
      found.add(symbol);
    }
  }

  return [...found].sort();
}

function checkNativeBinary(binaryPath, runNm = spawnSync) {
  const result = runNm("nm", ["-D", "--undefined-only", binaryPath], {
    encoding: "utf8",
  });

  if (result.error) {
    throw new Error(`Unable to inspect ${binaryPath}: ${result.error.message}`);
  }
  if (result.status !== 0) {
    throw new Error(
      `Unable to inspect ${binaryPath}: nm exited with status ${result.status}\n${result.stderr}`,
    );
  }

  const forbidden = findForbiddenUndefinedSymbols(result.stdout);
  if (forbidden.length > 0) {
    throw new Error(
      `${binaryPath} contains unresolved internal Lance symbols: ${forbidden.join(
        ", ",
      )}`,
    );
  }
}

function main() {
  const binaryPath = path.resolve(
    __dirname,
    "..",
    "npm",
    "linux-x64-musl",
    "lancedb.linux-x64-musl.node",
  );

  if (!existsSync(binaryPath)) {
    throw new Error(
      `Missing ${binaryPath}; assemble the native artifacts before publishing`,
    );
  }

  checkNativeBinary(binaryPath);
}

if (require.main === module) {
  try {
    main();
  } catch (error) {
    console.error(error instanceof Error ? error.message : error);
    process.exitCode = 1;
  }
}

module.exports = { checkNativeBinary, findForbiddenUndefinedSymbols };
