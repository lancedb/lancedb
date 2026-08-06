// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import packageJson = require("../package.json");

describe("package metadata", () => {
  it("requires Node.js type declarations compatible with the runtime", () => {
    expect(packageJson.engines.node).toBe(">= 18");
    expect(packageJson.peerDependencies["@types/node"]).toBe(">=18");
    expect(packageJson.peerDependenciesMeta["@types/node"]).toEqual({
      optional: true,
    });
  });
});
