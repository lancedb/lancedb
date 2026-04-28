import { open_table, type WasmRemoteSearchHandle } from "./generated/lancedb_wasm.js";
import type { WorkerRequest, WorkerResponse } from "./worker_protocol.js";

const scope = globalThis as typeof globalThis & {
  addEventListener(
    type: "message",
    listener: (event: MessageEvent<WorkerRequest>) => void,
  ): void;
  postMessage(message: WorkerResponse, transfer?: Transferable[]): void;
};

let handle: WasmRemoteSearchHandle | null = null;

scope.addEventListener("message", (event: MessageEvent<WorkerRequest>) => {
  void handleMessage(event.data);
});

async function handleMessage(request: WorkerRequest): Promise<void> {
  try {
    switch (request.type) {
      case "open":
        handle = await open_table(request.tableUrl, request.optionsJson);
        postResponse({
          id: request.id,
          ok: true,
          type: "open",
        });
        return;
      case "schema":
        postBytesResponse(request.id, "schema", await handleRef().schema());
        return;
      case "search":
        postBytesResponse(
          request.id,
          "search",
          await handleRef().search(request.requestJson),
        );
        return;
      case "refresh":
        postResponse({
          id: request.id,
          ok: true,
          type: "refresh",
          changed: await handleRef().refresh(),
        });
        return;
      default:
        request satisfies never;
    }
  } catch (error) {
    postResponse({
      id: request.id,
      ok: false,
      error: error instanceof Error ? error.message : String(error),
    });
  }
}

function handleRef(): WasmRemoteSearchHandle {
  if (handle === null) {
    throw new Error("Remote search worker is not open");
  }
  return handle;
}

function postBytesResponse(
  id: number,
  type: "schema" | "search",
  bytes: Uint8Array,
): void {
  const ownsBuffer = bytes.byteOffset === 0 && bytes.byteLength === bytes.buffer.byteLength;
  const transfer = (ownsBuffer ? bytes.buffer : bytes.slice().buffer) as ArrayBuffer;
  const message: WorkerResponse = {
    id,
    ok: true,
    type,
    bytes: transfer,
  };
  scope.postMessage(message, [transfer]);
}

function postResponse(response: WorkerResponse): void {
  scope.postMessage(response);
}
