// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import * as tmp from "tmp";

import {
  ColumnNotFoundError,
  FieldNotFoundError,
  HttpError,
  InvalidInputError,
  InvalidTableNameError,
  LanceError,
  TableAlreadyExistsError,
  TableNotFoundError,
  connect,
  isAlreadyExistsError,
  isLanceError,
} from "../lancedb";
import { materializeLanceError } from "../lancedb/errors";

describe("materializeLanceError", () => {
  it("rebuilds a native header as a typed error and keeps the cause chain", () => {
    const cause = new Error(
      'LanceError {"code":"Http","requestId":"req-1","statusCode":404}\nHttp error: (request_id=req-1) missing',
    );
    const raw = new Error(
      'LanceError {"code":"TableNotFound","tableName":"missing"}\nTable \'missing\' was not found',
    );
    raw.cause = cause;

    const error = materializeLanceError(raw);

    expect(error).toBeInstanceOf(TableNotFoundError);
    expect(error).toBeInstanceOf(LanceError);
    expect(error).toBeInstanceOf(Error);
    if (!(error instanceof TableNotFoundError)) {
      throw error;
    }
    expect(error.code).toBe("TableNotFound");
    expect(error.name).toBe("TableNotFoundError");
    expect(error.message).toBe("Table 'missing' was not found");
    expect(error.message).not.toContain("Caused by:");
    expect(error.message).not.toContain("LanceError");
    expect(error.tableName).toBe("missing");
    expect(error.cause).toBeInstanceOf(HttpError);
    if (!(error.cause instanceof HttpError)) {
      throw error.cause;
    }
    expect(error.cause.code).toBe("Http");
    expect(error.cause.requestId).toBe("req-1");
    expect(error.cause.statusCode).toBe(404);
    expect(error.cause.message).toBe("Http error: (request_id=req-1) missing");
    expect(error.stack).toContain(
      "TableNotFoundError: Table 'missing' was not found",
    );
  });

  it("maps field misses onto ColumnNotFoundError and preserves the specific code", () => {
    const error = materializeLanceError(
      new Error(
        'LanceError {"code":"FieldNotFound","columnName":"vector"}\nNo field named vector',
      ),
    );

    expect(error).toBeInstanceOf(FieldNotFoundError);
    expect(error).toBeInstanceOf(ColumnNotFoundError);
    if (!(error instanceof FieldNotFoundError)) {
      throw error;
    }
    expect(error.code).toBe("FieldNotFound");
    expect(error.columnName).toBe("vector");
  });

  it("keeps an unknown Rust kind as LanceError with that code", () => {
    const error = materializeLanceError(
      new Error('LanceError {"code":"FutureKind"}\nnot classified yet'),
    );

    expect(error).toBeInstanceOf(LanceError);
    expect(error).not.toBeInstanceOf(TableNotFoundError);
    if (!(error instanceof LanceError)) {
      throw error;
    }
    expect(error.code).toBe("FutureKind");
    expect(error.message).toBe("not classified yet");
  });

  it("leaves TypeScript errors that are not from Rust untouched", () => {
    const error = new Error("Table some_table is closed");
    expect(materializeLanceError(error)).toBe(error);
    expect(isLanceError(error)).toBe(false);
  });

  it("treats invalid table names as invalid input", () => {
    const error = new InvalidTableNameError("bad name");
    expect(error).toBeInstanceOf(InvalidInputError);
    expect(error.code).toBe("InvalidTableName");
    expect(isLanceError(error)).toBe(true);
    expect(isAlreadyExistsError(error)).toBe(false);
  });
});

describe("native LanceDB errors", () => {
  let tmpDir: tmp.DirResult;

  beforeEach(() => {
    tmpDir = tmp.dirSync({ unsafeCleanup: true });
  });

  afterEach(() => {
    tmpDir.removeCallback();
  });

  it("throws TableNotFoundError when opening a missing table", async () => {
    const db = await connect(tmpDir.name);

    await expect(db.openTable("missing")).rejects.toBeInstanceOf(
      TableNotFoundError,
    );

    try {
      await db.openTable("missing");
      throw new Error("expected openTable to fail");
    } catch (error) {
      expect(isLanceError(error)).toBe(true);
      if (!(error instanceof TableNotFoundError)) {
        throw error;
      }
      expect(error.code).toBe("TableNotFound");
      expect(error.tableName).toBe("missing");
      expect(error.message).toContain("missing");
      expect(error.message).not.toContain("LanceError {");
      expect(error.message).not.toContain("Caused by:");
    }
  });

  it("throws TableAlreadyExistsError when creating a table twice", async () => {
    const db = await connect(tmpDir.name);
    await db.createTable("docs", [{ id: 1 }]);

    await expect(db.createTable("docs", [{ id: 2 }])).rejects.toBeInstanceOf(
      TableAlreadyExistsError,
    );

    try {
      await db.createTable("docs", [{ id: 3 }]);
      throw new Error("expected createTable to fail");
    } catch (error) {
      expect(isAlreadyExistsError(error)).toBe(true);
      if (!(error instanceof TableAlreadyExistsError)) {
        throw error;
      }
      expect(error.code).toBe("TableAlreadyExists");
      expect(error.tableName).toBe("docs");
      expect(error.message).toContain("already exists");
    }
  });

  it("throws ColumnNotFoundError when a filter names a missing column", async () => {
    const db = await connect(tmpDir.name);
    const table = await db.createTable("docs", [{ id: 1, vector: [0.1, 0.2] }]);

    await expect(
      table.query().where("missing > 1").toArray(),
    ).rejects.toBeInstanceOf(ColumnNotFoundError);
  });
});
