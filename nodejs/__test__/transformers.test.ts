// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import { execFileSync } from "node:child_process";
import { resolve } from "node:path";

describe("Transformers embedding tokenizer selection", () => {
  // Run init's native ESM import outside Jest's CommonJS sandbox, like a user.
  // The fixture mocks model loading and forbids local/remote model access.
  test.each([
    ["default model", 0],
    ["default tokenizer", 1],
    ["custom tokenizer", 2],
    ["tokenizer registry variable", 3],
    ["tokenizer loading error", 4],
  ])("handles %s", (_name, testCase) => {
    execFileSync(
      process.execPath,
      [
        resolve(__dirname, "fixtures", "transformers_tokenizer.cjs"),
        String(testCase),
      ],
      { stdio: "pipe", timeout: 15_000 },
    );
  });
});
