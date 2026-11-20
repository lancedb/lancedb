// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import * as vm from "node:vm";
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
import { LANCE_ERROR_MARK, materializeLanceError } from "../lancedb/errors";

type LanceDbModule = typeof import("../lancedb");
type ErrorsModule = typeof import("../lancedb/errors");

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

  it("rebuilds an error already translated by another module realm", () => {
    const cause = foreignLanceError({
      name: "HttpError",
      code: "Http",
      message: "Http error: (request_id=req-1) missing",
      requestId: "req-1",
      statusCode: 404,
    });
    const foreign = foreignLanceError({
      name: "TableNotFoundError",
      code: "TableNotFound",
      message: "Table 'missing' was not found",
      tableName: "missing",
      cause,
    });

    const error = materializeLanceError(foreign);

    expect(error).toBeInstanceOf(TableNotFoundError);
    expect(error).not.toBe(foreign);
    if (!(error instanceof TableNotFoundError)) {
      throw error;
    }
    expect(error.code).toBe("TableNotFound");
    expect(error.tableName).toBe("missing");
    expect(error.message).toBe("Table 'missing' was not found");
    expect(error.message).not.toContain("LanceError {");
    expect(error.cause).toBeInstanceOf(HttpError);
    if (!(error.cause instanceof HttpError)) {
      throw error.cause;
    }
    expect(error.cause.code).toBe("Http");
    expect(error.cause.requestId).toBe("req-1");
    expect(error.cause.statusCode).toBe(404);
    expect(error.cause.message).toBe("Http error: (request_id=req-1) missing");
  });

  it("does not treat an unmarked foreign error as a LanceError", () => {
    const foreign = vm.runInNewContext(
      `(() => {
        const error = new Error("Table 'missing' was not found");
        error.code = "TableNotFound";
        error.tableName = "missing";
        return error;
      })()`,
    );

    const error = materializeLanceError(foreign);

    expect(error).not.toBeInstanceOf(LanceError);
    expect(error).not.toBeInstanceOf(TableNotFoundError);
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

  it("keeps each SDK load's subclasses after another load has translated an error", async () => {
    const firstDir = tmp.dirSync({ unsafeCleanup: true });
    const secondDir = tmp.dirSync({ unsafeCleanup: true });
    try {
      let openMissing!: () => Promise<TableNotFoundError>;
      let firstTableNotFound!: LanceDbModule["TableNotFoundError"];
      jest.isolateModules(() => {
        const lancedb = require("../lancedb") as LanceDbModule;
        firstTableNotFound = lancedb.TableNotFoundError;
        openMissing = async () => {
          const db = await lancedb.connect(firstDir.name);
          try {
            await db.openTable("missing");
            throw new Error("expected openTable to fail");
          } catch (error) {
            expect(error).toBeInstanceOf(lancedb.TableNotFoundError);
            if (!(error instanceof lancedb.TableNotFoundError)) {
              throw error;
            }
            expect(error.code).toBe("TableNotFound");
            expect(error.tableName).toBe("missing");
            expect(error.message).not.toContain("LanceError {");
            expect(error.message).not.toContain("Caused by:");
            return error;
          }
        };
      });
      const firstError = await openMissing();

      let checkLaterLoad!: (previous: unknown) => Promise<void>;
      let laterTableNotFound!: LanceDbModule["TableNotFoundError"];
      jest.isolateModules(() => {
        const lancedb = require("../lancedb") as LanceDbModule;
        laterTableNotFound = lancedb.TableNotFoundError;
        const { materializeLanceError: materialize } =
          require("../lancedb/errors") as ErrorsModule;
        checkLaterLoad = async (previous: unknown) => {
          const rebuilt = materialize(previous);
          expect(rebuilt).toBeInstanceOf(lancedb.TableNotFoundError);
          expect(rebuilt).not.toBe(previous);
          if (!(rebuilt instanceof lancedb.TableNotFoundError)) {
            throw rebuilt;
          }
          expect(rebuilt.code).toBe("TableNotFound");
          expect(rebuilt.tableName).toBe("missing");
          expect(rebuilt.message).toBe(firstError.message);

          const db = await lancedb.connect(secondDir.name);
          try {
            await db.openTable("missing");
            throw new Error("expected openTable to fail");
          } catch (error) {
            expect(error).toBeInstanceOf(lancedb.TableNotFoundError);
            if (!(error instanceof lancedb.TableNotFoundError)) {
              throw error;
            }
            expect(error.code).toBe("TableNotFound");
            expect(error.tableName).toBe("missing");
            expect(error.message).not.toContain("LanceError {");
          }

          await db.createTable("docs", [{ id: 1 }]);
          try {
            await db.createTable("docs", [{ id: 2 }]);
            throw new Error("expected createTable to fail");
          } catch (error) {
            expect(error).toBeInstanceOf(lancedb.TableAlreadyExistsError);
            if (!(error instanceof lancedb.TableAlreadyExistsError)) {
              throw error;
            }
            expect(error.code).toBe("TableAlreadyExists");
            expect(error.tableName).toBe("docs");
          }

          const table = await db.openTable("docs");
          try {
            await table.query().where("missing > 1").toArray();
            throw new Error("expected query to fail");
          } catch (error) {
            expect(error).toBeInstanceOf(lancedb.LanceError);
            expect(error).toBeInstanceOf(lancedb.ColumnNotFoundError);
            if (!(error instanceof lancedb.ColumnNotFoundError)) {
              throw error;
            }
            expect(error.message).not.toContain("LanceError {");
          }
        };
      });
      await checkLaterLoad(firstError);

      const stillFirst = await openMissing();
      expect(stillFirst).toBeInstanceOf(firstTableNotFound);
      expect(stillFirst.tableName).toBe("missing");
      expect(stillFirst).not.toBeInstanceOf(laterTableNotFound);
      expect(stillFirst).not.toBeInstanceOf(TableNotFoundError);
    } finally {
      firstDir.removeCallback();
      secondDir.removeCallback();
    }
  });
});

function foreignLanceError(fields: {
  name: string;
  code: string;
  message: string;
  cause?: unknown;
  requestId?: string;
  statusCode?: number;
  tableName?: string;
}): Error {
  return vm.runInNewContext(
    `(() => {
      class ForeignError extends Error {
        constructor(message, cause) {
          super(message, { cause });
          this.name = name;
          this.code = code;
          if (requestId !== undefined) this.requestId = requestId;
          if (statusCode !== undefined) this.statusCode = statusCode;
          if (tableName !== undefined) this.tableName = tableName;
          Object.defineProperty(this, mark, { value: true });
        }
      }
      return new ForeignError(message, cause);
    })()`,
    {
      name: fields.name,
      code: fields.code,
      message: fields.message,
      cause: fields.cause,
      requestId: fields.requestId,
      statusCode: fields.statusCode,
      tableName: fields.tableName,
      mark: LANCE_ERROR_MARK,
    },
  ) as Error;
}
