const path = require("node:path");

// Set the real process environment before Jest creates its sandbox and workers.
// Assigning process.env inside a test does not reach Rust's std::env.
process.env.LANCEDB_OAUTH_BROWSER =
  process.platform === "win32"
    ? path.join(__dirname, "__test__/fixtures/oauth_browser.cmd")
    : "/usr/bin/true";

/** @type {import('ts-jest').JestConfigWithTsJest} */
module.exports = {
  preset: "ts-jest",
  testEnvironment: "node",
  moduleDirectories: ["node_modules", "./dist"],
  moduleFileExtensions: ["js", "ts"],
  modulePathIgnorePatterns: ["<rootDir>/examples/"],
};
