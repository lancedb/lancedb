// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

// The `src/extension.ts` of the sample extension in the VS Code integration guide.
// It needs the VS Code extension host, so jest doesn't run it: the LanceDB code it
// calls is tested in vscode_search.test.ts, and the extension itself was tested
// with @vscode/test-cli.

// --8<-- [start:vscode_extension]
import * as vscode from "vscode";
import {
  type Chunk,
  chunkMarkdown,
  indexChunks,
  openDatabase,
  searchChunks,
} from "./search";

export async function activate(context: vscode.ExtensionContext) {
  // One index per workspace. `storageUri` is undefined when no folder is open.
  const storage = context.storageUri ?? context.globalStorageUri;

  // Transformers.js downloads the embedding model on first use. Keep it in
  // global storage so every workspace shares one copy and it survives
  // extension updates. Use `import()`: LanceDB loads the ES module build, and
  // `require()` would return a separate copy whose settings LanceDB never sees.
  const { env } = await import("@huggingface/transformers");
  env.cacheDir = vscode.Uri.joinPath(context.globalStorageUri, "models").fsPath;

  context.subscriptions.push(
    vscode.commands.registerCommand("lancedbSearch.index", async () => {
      const files = await vscode.workspace.findFiles(
        "**/*.md",
        "**/node_modules/**",
      );
      const chunks: Chunk[] = [];
      for (const file of files) {
        const bytes = await vscode.workspace.fs.readFile(file);
        const relativePath = vscode.workspace.asRelativePath(file, false);
        chunks.push(
          ...chunkMarkdown(relativePath, new TextDecoder().decode(bytes)),
        );
      }
      if (chunks.length === 0) {
        vscode.window.showWarningMessage("No Markdown files to index.");
        return 0;
      }

      await vscode.window.withProgress(
        {
          location: vscode.ProgressLocation.Notification,
          title: `Indexing ${files.length} Markdown files`,
        },
        async () => {
          await vscode.workspace.fs.createDirectory(storage);
          await indexChunks(await openDatabase(storage.fsPath), chunks);
        },
      );
      vscode.window.showInformationMessage(
        `Indexed ${chunks.length} paragraphs from ${files.length} files.`,
      );
      return chunks.length;
    }),

    // Called with a query (for example from a test or another extension), the
    // command returns the matching rows instead of showing a picker.
    vscode.commands.registerCommand(
      "lancedbSearch.search",
      async (query?: string) => {
        const text =
          query ??
          (await vscode.window.showInputBox({
            prompt: "Search Markdown by meaning",
          }));
        if (!text) {
          return [];
        }
        const results = await searchChunks(
          await openDatabase(storage.fsPath),
          text,
        );
        if (query !== undefined) {
          return results;
        }

        const picked = await vscode.window.showQuickPick(
          results.map((row) => ({
            label: `${row.path}:${row.line + 1}`,
            description: `distance ${row._distance.toFixed(3)}`,
            detail: row.text,
            row,
          })),
          { placeHolder: text, matchOnDetail: true },
        );
        const folder = vscode.workspace.workspaceFolders?.[0];
        if (picked && folder) {
          const editor = await vscode.window.showTextDocument(
            vscode.Uri.joinPath(folder.uri, picked.row.path),
          );
          const start = new vscode.Position(picked.row.line, 0);
          editor.selection = new vscode.Selection(start, start);
          editor.revealRange(
            new vscode.Range(start, start),
            vscode.TextEditorRevealType.InCenter,
          );
        }
        return results;
      },
    ),
  );
}

export function deactivate() {}
// --8<-- [end:vscode_extension]
