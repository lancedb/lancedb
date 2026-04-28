export function createDefaultWorker() {
  return new Worker(new URL("./worker.js", import.meta.url), {
    type: "module",
  });
}
