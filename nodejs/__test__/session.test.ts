// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import * as tmp from "tmp";
import { Session, connect } from "../lancedb";

describe("Session", () => {
  let tmpDir: tmp.DirResult;
  beforeEach(() => {
    tmpDir = tmp.dirSync({ unsafeCleanup: true });
  });
  afterEach(() => tmpDir.removeCallback());

  it("should configure cache sizes and work with database operations", async () => {
    // Create session with small cache limits for testing
    const indexCacheSize = BigInt(1024 * 1024); // 1MB
    const metadataCacheSize = BigInt(512 * 1024); // 512KB

    const session = new Session(indexCacheSize, metadataCacheSize);

    // Record initial cache state
    const initialCacheSize = session.sizeBytes();
    const initialCacheItems = session.approxNumItems();

    // Test session works with database connection
    const db = await connect({ uri: tmpDir.name, session: session });

    // Create and use a table to exercise the session
    const data = Array.from({ length: 100 }, (_, i) => ({
      id: i,
      text: `item ${i}`,
    }));
    const table = await db.createTable("test", data);
    const results = await table.query().limit(5).toArray();

    expect(results).toHaveLength(5);

    // Verify cache usage increased after operations
    const finalCacheSize = session.sizeBytes();
    const finalCacheItems = session.approxNumItems();

    expect(finalCacheSize).toBeGreaterThan(initialCacheSize); // Cache should have grown
    expect(finalCacheItems).toBeGreaterThanOrEqual(initialCacheItems); // Items should not decrease
    expect(initialCacheSize).toBeLessThan(indexCacheSize + metadataCacheSize); // Within limits
  });
});
