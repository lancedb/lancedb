export interface WasmRemoteSearchHandle {
  schema(): Promise<Uint8Array>;
  search(requestJson: string): Promise<Uint8Array>;
  refresh(): Promise<boolean>;
  close(): void;
}

export interface WasmModule {
  open_table(
    tableUrl: string,
    optionsJson?: string,
  ): Promise<WasmRemoteSearchHandle>;
}

type RuntimeModule = {
  default?: (input?: URL | RequestInfo | Response | BufferSource | WebAssembly.Module) => Promise<unknown>;
  open_table(
    tableUrl: string,
    optionsJson?: string,
  ): Promise<WasmRemoteSearchHandle>;
};

let runtimeModulePromise: Promise<RuntimeModule> | null = null;

async function loadRuntimeModule(): Promise<RuntimeModule> {
  if (runtimeModulePromise === null) {
    runtimeModulePromise = (async () => {
      let runtime: RuntimeModule;
      try {
        runtime = (await import("./lancedb_wasm_runtime.js")) as RuntimeModule;
      } catch (error) {
        throw new Error(
          `The generated lancedb_wasm artifact is missing. Run \`npm run build\` in the web package before using @lancedb/lancedb-web at runtime. ${(error as Error).message}`,
        );
      }

      if (runtime.default !== undefined) {
        await runtime.default();
      }
      return runtime;
    })();
  }

  return runtimeModulePromise;
}

export async function open_table(
  tableUrl: string,
  optionsJson?: string,
): Promise<WasmRemoteSearchHandle> {
  const runtime = await loadRuntimeModule();
  return runtime.open_table(tableUrl, optionsJson);
}
