import * as fs from "fs";
import * as path from "path";
import { tmpdir } from "os";

export async function withTempDirectory(
  fn: (tempDir: string) => Promise<void>,
) {
  const tmpDirPath = fs.mkdtempSync(path.join(tmpdir(), "temp-dir-"));
  try {
    await fn(tmpDirPath);
  } finally {
    fs.rmdirSync(tmpDirPath, { recursive: true });
  }
}
