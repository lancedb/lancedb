export default function init(
  input?: URL | RequestInfo | Response | BufferSource | WebAssembly.Module,
): Promise<unknown>;

export function open_table(
  tableUrl: string,
  optionsJson?: string,
): Promise<{
  schema(): Promise<Uint8Array>;
  search(requestJson: string): Promise<Uint8Array>;
  refresh(): Promise<boolean>;
  close(): void;
}>;
