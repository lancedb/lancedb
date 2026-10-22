// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

const assert = require("node:assert/strict");
const {
  TransformersEmbeddingFunction,
} = require("../../dist/embedding/transformers");
const { getRegistry } = require("../../dist/embedding/registry");

async function main() {
  const transformers = await import("@huggingface/transformers");
  // Never fetch a real model, including if a loader mock stops intercepting.
  transformers.env.allowRemoteModels = false;
  transformers.env.allowLocalModels = false;
  const modelCalls = [];
  const tokenizerCalls = [];
  const testCase = Number(process.argv[2]);
  const defaults = "Xenova/all-MiniLM-L6-v2";
  const cases = [
    [{}, defaults, defaults],
    [{ model: "test/model" }, "test/model", "test/model"],
    [
      { model: "test/model", tokenizer: "test/tokenizer" },
      "test/model",
      "test/tokenizer",
    ],
    [
      { model: "test/model", tokenizer: "$var:tokenizer" },
      "test/model",
      "test/tokenizer",
    ],
    [
      { model: "test/model", tokenizer: "test/tokenizer" },
      "test/model",
      "test/tokenizer",
    ],
  ];
  assert.ok(
    Number.isInteger(testCase) && testCase >= 0 && testCase < cases.length,
  );
  transformers.AutoModel.from_pretrained = async (...args) => {
    modelCalls.push(args);
    return {};
  };
  transformers.AutoTokenizer.from_pretrained = async (...args) => {
    tokenizerCalls.push(args);
    if (testCase === 4) throw new Error("missing tokenizer");
    return {};
  };
  getRegistry().setVar("tokenizer", "test/tokenizer");
  const [options, model, tokenizer] = cases[testCase];
  const fn = new TransformersEmbeddingFunction(options);
  if (testCase === 4) {
    await assert.rejects(fn.init(), /error loading tokenizer test\/tokenizer/);
  } else {
    await fn.init();
  }
  assert.deepEqual(modelCalls, [[model, { dtype: "fp32" }]]);
  assert.deepEqual(tokenizerCalls, [[tokenizer]]);
  assert.deepEqual(fn.toJSON(), options);
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
