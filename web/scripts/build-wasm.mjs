import { mkdirSync, rmSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";

const WASM_BINDGEN_VERSION = "0.2.115";
const __dirname = dirname(fileURLToPath(import.meta.url));
const webRoot = resolve(__dirname, "..");
const repoRoot = resolve(webRoot, "..");
const distDir = resolve(webRoot, "dist");
const generatedDir = resolve(distDir, "generated");
const toolsRoot = resolve(webRoot, ".wasm-tools");
const toolBinDir = resolve(toolsRoot, "bin");
const wasmBindgenBin = resolve(toolBinDir, "wasm-bindgen");
const targetWasm = resolve(
  repoRoot,
  "target",
  "wasm32-unknown-unknown",
  "release",
  "lancedb_wasm.wasm",
);

rmSync(generatedDir, { force: true, recursive: true });
mkdirSync(generatedDir, { recursive: true });

ensureWasmBindgen();
buildRustWasm();
runWasmBindgen();

function ensureWasmBindgen() {
  const check = spawnSync(wasmBindgenBin, ["--version"], {
    stdio: "ignore",
  });
  if (check.status === 0) {
    return;
  }

  run(
    "cargo",
    [
      "install",
      "wasm-bindgen-cli",
      "--version",
      WASM_BINDGEN_VERSION,
      "--locked",
      "--root",
      toolsRoot,
    ],
    repoRoot,
  );
}

function buildRustWasm() {
  const existingRustflags = process.env.RUSTFLAGS?.trim();
  const getrandomFlag = '--cfg getrandom_backend="wasm_js"';
  run(
    "cargo",
    [
      "build",
      "-p",
      "lancedb-wasm",
      "--target",
      "wasm32-unknown-unknown",
      "--release",
    ],
    repoRoot,
    {
      ...process.env,
      RUSTFLAGS: existingRustflags
        ? `${existingRustflags} ${getrandomFlag}`
        : getrandomFlag,
    },
  );
}

function runWasmBindgen() {
  run(
    wasmBindgenBin,
    [
      "--target",
      "web",
      "--out-dir",
      generatedDir,
      "--out-name",
      "lancedb_wasm_runtime",
      targetWasm,
    ],
    repoRoot,
  );
}

function run(command, args, cwd, env = process.env) {
  const result = spawnSync(command, args, {
    cwd,
    env,
    stdio: "inherit",
  });
  if (result.status !== 0) {
    throw new Error(`${command} ${args.join(" ")} failed with exit code ${result.status}`);
  }
}
