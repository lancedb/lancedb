// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors
import { expect, test } from "@jest/globals";
import { withTempDirectory } from "./util.ts";
import {
  chunkMarkdown,
  indexChunks,
  openDatabase,
  searchChunks,
} from "./vscode_search.ts";

// The same files make up the sample workspace used in the docs page.
const workspace: Record<string, string> = {
  "README.md":
    "# Acme CLI\n\nAcme CLI syncs your local notes to the Acme cloud.\n\nInstall it with `npm install -g acme-cli`, then run `acme login`.\n",
  "docs/auth.md":
    "# Authentication\n\nAcme uses personal access tokens. Create one under Settings > Tokens.\n\nIf a token leaks, revoke it on the Tokens page and generate a replacement.\nRevoked tokens stop working immediately.\n",
  "docs/sync.md":
    "# Syncing\n\nRun `acme sync` to upload changed notes. Only files modified since the last sync are sent.\n\nConflicts are resolved by keeping both copies and adding `.conflict` to the older one.\n",
  "docs/config.md":
    "# Configuration\n\nSettings live in `~/.acme/config.toml`.\n\nSet `sync.interval` to control how often background sync runs, in minutes.\n",
  "docs/troubleshooting.md":
    "# Troubleshooting\n\nIf `acme sync` hangs, check that your proxy allows connections to api.acme.dev on port 443.\n\nRun `acme doctor` to print diagnostics you can attach to a bug report.\n",
  "CONTRIBUTING.md":
    "# Contributing\n\nRun `npm test` before opening a pull request. New commands need a page in docs/.\n",
};

test("chunkMarkdown splits paragraphs and records their start line", () => {
  expect(chunkMarkdown("docs/auth.md", workspace["docs/auth.md"])).toEqual([
    { path: "docs/auth.md", line: 0, text: "# Authentication" },
    {
      path: "docs/auth.md",
      line: 2,
      text: "Acme uses personal access tokens. Create one under Settings > Tokens.",
    },
    {
      path: "docs/auth.md",
      line: 4,
      text: "If a token leaks, revoke it on the Tokens page and generate a replacement.\nRevoked tokens stop working immediately.",
    },
  ]);
});

test("index workspace Markdown and search it by meaning", async () => {
  await withTempDirectory(async (storageDir) => {
    const chunks = Object.entries(workspace).flatMap(([filePath, content]) =>
      chunkMarkdown(filePath, content),
    );
    await indexChunks(await openDatabase(storageDir), chunks);

    // Each command in the extension opens its own connection, so search
    // through a fresh one to check the table is usable after a reload.
    const db = await openDatabase(storageDir);
    const leaked = await searchChunks(
      db,
      "my credentials were exposed, what should I do?",
    );
    expect(leaked[0].path).toBe("docs/auth.md");
    expect(leaked[0].line).toBe(4);

    const firewall = await searchChunks(
      db,
      "uploads are stuck behind the company firewall",
    );
    expect(firewall[0].path).toBe("docs/troubleshooting.md");
  });
}, 100_000);
