const path = require("node:path");

// Set the real process environment before Jest creates its sandbox and workers.
// Assigning process.env inside a test does not reach Rust's std::env.
process.env.LANCEDB_OAUTH_BROWSER =
  process.platform === "win32"
    ? path.join(__dirname, "__test__/fixtures/oauth_browser.cmd")
    : "/usr/bin/true";

/** @type {import('jest').Config} */
module.exports = {
  transform: {
    "^.+\\.ts$": [
      "@swc/jest",
      {
        jsc: {
          target: "es2022",
          parser: { syntax: "typescript", decorators: true },
          transform: { legacyDecorator: true, decoratorMetadata: true },
        },
      },
    ],
  },
  testEnvironment: "node",
  moduleDirectories: ["node_modules", "./dist"],
  moduleFileExtensions: ["js", "ts"],
  modulePathIgnorePatterns: ["<rootDir>/examples/"],
};
