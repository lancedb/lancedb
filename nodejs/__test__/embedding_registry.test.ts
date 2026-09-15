// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import { execFileSync } from "node:child_process";
import { resolve } from "node:path";

import type { OpenAIEmbeddingFunction } from "../lancedb/embedding/openai";
import type { EmbeddingFunctionRegistry } from "../lancedb/embedding/registry";

type EmbeddingModule = typeof import("../lancedb/embedding");
type OpenAIModule = typeof import("../lancedb/embedding/openai");
type RegistryModule = typeof import("../lancedb/embedding/registry");

describe("embedding function registry", () => {
  const registries: EmbeddingFunctionRegistry[] = [];

  afterEach(() => {
    for (const registry of registries) {
      registry.reset();
    }
    registries.length = 0;
  });

  it("defers built-in providers until the public registry API is used", () => {
    jest.isolateModules(() => {
      const embedding = require("../lancedb/embedding") as EmbeddingModule;
      const { getRegistry: getInternalRegistry } =
        require("../lancedb/embedding/registry") as RegistryModule;
      const registry = getInternalRegistry();
      registries.push(registry);

      expect(registry.length()).toBe(0);
      expect(embedding.getRegistry()).toBe(registry);
      expect(registry.get("openai")).toBeDefined();
      expect(registry.get("huggingface")).toBeDefined();
    });
  });

  it("preserves automatic FTS search in a fresh process", () => {
    execFileSync(
      process.execPath,
      [resolve(__dirname, "fixtures", "auto_fts_search.cjs")],
      { stdio: "pipe" },
    );
  });

  it("shares registrations across duplicated provider module graphs", () => {
    let registeringRegistry: EmbeddingFunctionRegistry | undefined;
    let latestOpenAIConstructor: typeof OpenAIEmbeddingFunction | undefined;

    jest.isolateModules(() => {
      require("../lancedb/embedding/openai");
      const { getRegistry } =
        require("../lancedb/embedding/registry") as RegistryModule;
      registeringRegistry = getRegistry();
      registries.push(registeringRegistry);
      expect(registeringRegistry.get("openai")).toBeDefined();
    });

    expect(() => {
      jest.isolateModules(() => {
        const { OpenAIEmbeddingFunction } =
          require("../lancedb/embedding/openai") as OpenAIModule;
        latestOpenAIConstructor = OpenAIEmbeddingFunction;
        const { getRegistry } =
          require("../lancedb/embedding/registry") as RegistryModule;
        registries.push(getRegistry());
      });
    }).not.toThrow();

    const previousApiKey = process.env.OPENAI_API_KEY;
    process.env.OPENAI_API_KEY = "test";
    try {
      const latestOpenAI = registeringRegistry!
        .get<OpenAIEmbeddingFunction>("openai")!
        .create();
      expect(latestOpenAI).toBeInstanceOf(latestOpenAIConstructor!);
    } finally {
      if (previousApiKey === undefined) {
        delete process.env.OPENAI_API_KEY;
      } else {
        process.env.OPENAI_API_KEY = previousApiKey;
      }
    }

    jest.isolateModules(() => {
      const { getRegistry } =
        require("../lancedb/embedding") as EmbeddingModule;
      const publicRegistry = getRegistry();
      registries.push(publicRegistry);
      expect(publicRegistry).toBe(registeringRegistry);
      expect(publicRegistry.get("openai")).toBeDefined();
    });
  });
});
