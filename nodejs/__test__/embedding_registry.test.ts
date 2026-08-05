// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import type { EmbeddingFunctionRegistry } from "../lancedb/embedding/registry";

type EmbeddingModule = typeof import("../lancedb/embedding");
type RegistryModule = typeof import("../lancedb/embedding/registry");

describe("embedding function registry", () => {
  const registries: EmbeddingFunctionRegistry[] = [];

  afterEach(() => {
    for (const registry of registries) {
      registry.reset();
    }
    registries.length = 0;
  });

  it("shares registrations across isolated module instances", () => {
    let registeringRegistry: EmbeddingFunctionRegistry | undefined;

    jest.isolateModules(() => {
      require("../lancedb/embedding/openai");
      const { getRegistry } =
        require("../lancedb/embedding/registry") as RegistryModule;
      registeringRegistry = getRegistry();
      registries.push(registeringRegistry);
      expect(registeringRegistry.get("openai")).toBeDefined();
    });

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
