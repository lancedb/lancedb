// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

const create = jest.fn();

jest.mock("openai", () =>
  jest.fn().mockImplementation(() => ({ embeddings: { create } })),
);

import { OpenAIEmbeddingFunction } from "../lancedb/embedding/openai";
import { getRegistry } from "../lancedb/embedding/registry";

describe("OpenAIEmbeddingFunction", () => {
  beforeEach(() => {
    create.mockReset();
    create.mockImplementation(async ({ input }: { input: string[] }) => {
      if (input.length > 2048) {
        throw new Error("400 max 2048 inputs per request");
      }
      return { data: input.map((_, i) => ({ embedding: [i] })) };
    });
  });

  it("splits source embeddings into batches of at most 2048 inputs", async () => {
    const registry = getRegistry();
    registry.setVar("openai_api_key", "sk-test");
    const func = new OpenAIEmbeddingFunction({
      apiKey: "$var:openai_api_key",
      model: "text-embedding-3-small",
    });
    const data = Array.from({ length: 5000 }, (_, i) => `row ${i}`);

    const embeddings = await func.computeSourceEmbeddings(data);

    expect(embeddings).toHaveLength(5000);
    expect(create.mock.calls.map(([args]) => args.input.length)).toEqual([
      2048, 2048, 904,
    ]);
  });
});
