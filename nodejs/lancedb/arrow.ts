// Copyright 2023 Lance Developers.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import {
  Table as ArrowTable,
  Binary,
  BufferType,
  DataType,
  Field,
  FixedSizeBinary,
  FixedSizeList,
  Float,
  Float32,
  Int,
  LargeBinary,
  List,
  Null,
  RecordBatch,
  RecordBatchFileWriter,
  RecordBatchStreamWriter,
  Schema,
  Struct,
  Utf8,
  Vector,
  makeBuilder,
  makeData,
  type makeTable,
  vectorFromArray,
} from "apache-arrow";
import { Buffers } from "apache-arrow/data";
import { type EmbeddingFunction } from "./embedding/embedding_function";
import { EmbeddingFunctionConfig, getRegistry } from "./embedding/registry";
import {
  sanitizeField,
  sanitizeSchema,
  sanitizeTable,
  sanitizeType,
} from "./sanitize";
export * from "apache-arrow";
export type SchemaLike =
  | Schema
  | {
      fields: FieldLike[];
      metadata: Map<string, string>;
      get names(): unknown[];
    };
export type FieldLike =
  | Field
  | {
      type: string;
      name: string;
      nullable?: boolean;
      metadata?: Map<string, string>;
    };

export type DataLike =
  // biome-ignore lint/suspicious/noExplicitAny: <explanation>
  | import("apache-arrow").Data<Struct<any>>
  | {
      // biome-ignore lint/suspicious/noExplicitAny: <explanation>
      type: any;
      length: number;
      offset: number;
      stride: number;
      nullable: boolean;
      children: DataLike[];
      get nullCount(): number;
      // biome-ignore lint/suspicious/noExplicitAny: <explanation>
      values: Buffers<any>[BufferType.DATA];
      // biome-ignore lint/suspicious/noExplicitAny: <explanation>
      typeIds: Buffers<any>[BufferType.TYPE];
      // biome-ignore lint/suspicious/noExplicitAny: <explanation>
      nullBitmap: Buffers<any>[BufferType.VALIDITY];
      // biome-ignore lint/suspicious/noExplicitAny: <explanation>
      valueOffsets: Buffers<any>[BufferType.OFFSET];
    };

export type RecordBatchLike =
  | RecordBatch
  | {
      schema: SchemaLike;
      data: DataLike;
    };

export type TableLike =
  | ArrowTable
  | { schema: SchemaLike; batches: RecordBatchLike[] };

export type IntoVector =
  | Float32Array
  | Float64Array
  | number[]
  | Promise<Float32Array | Float64Array | number[]>;

export function isArrowTable(value: object): value is TableLike {
  if (value instanceof ArrowTable) return true;
  return "schema" in value && "batches" in value;
}

export function isNull(value: unknown): value is Null {
  return value instanceof Null || DataType.isNull(value);
}
export function isInt(value: unknown): value is Int {
  return value instanceof Int || DataType.isInt(value);
}
export function isFloat(value: unknown): value is Float {
  return value instanceof Float || DataType.isFloat(value);
}
export function isBinary(value: unknown): value is Binary {
  return value instanceof Binary || DataType.isBinary(value);
}
export function isLargeBinary(value: unknown): value is LargeBinary {
  return value instanceof LargeBinary || DataType.isLargeBinary(value);
}
export function isUtf8(value: unknown): value is Utf8 {
  return value instanceof Utf8 || DataType.isUtf8(value);
}
export function isLargeUtf8(value: unknown): value is Utf8 {
  return value instanceof Utf8 || DataType.isLargeUtf8(value);
}
export function isBool(value: unknown): value is Utf8 {
  return value instanceof Utf8 || DataType.isBool(value);
}
export function isDecimal(value: unknown): value is Utf8 {
  return value instanceof Utf8 || DataType.isDecimal(value);
}
export function isDate(value: unknown): value is Utf8 {
  return value instanceof Utf8 || DataType.isDate(value);
}
export function isTime(value: unknown): value is Utf8 {
  return value instanceof Utf8 || DataType.isTime(value);
}
export function isTimestamp(value: unknown): value is Utf8 {
  return value instanceof Utf8 || DataType.isTimestamp(value);
}
export function isInterval(value: unknown): value is Utf8 {
  return value instanceof Utf8 || DataType.isInterval(value);
}
export function isDuration(value: unknown): value is Utf8 {
  return value instanceof Utf8 || DataType.isDuration(value);
}
export function isList(value: unknown): value is List {
  return value instanceof List || DataType.isList(value);
}
export function isStruct(value: unknown): value is Struct {
  return value instanceof Struct || DataType.isStruct(value);
}
export function isUnion(value: unknown): value is Struct {
  return value instanceof Struct || DataType.isUnion(value);
}
export function isFixedSizeBinary(value: unknown): value is FixedSizeBinary {
  return value instanceof FixedSizeBinary || DataType.isFixedSizeBinary(value);
}

export function isFixedSizeList(value: unknown): value is FixedSizeList {
  return value instanceof FixedSizeList || DataType.isFixedSizeList(value);
}

/** Data type accepted by NodeJS SDK */
export type Data = Record<string, unknown>[] | TableLike;

/*
 * Options to control how a column should be converted to a vector array
 */
export class VectorColumnOptions {
  /** Vector column type. */
  type: Float = new Float32();

  constructor(values?: Partial<VectorColumnOptions>) {
    Object.assign(this, values);
  }
}

/** Options to control the makeArrowTable call. */
export class MakeArrowTableOptions {
  /*
   * Schema of the data.
   *
   * If this is not provided then the data type will be inferred from the
   * JS type.  Integer numbers will become int64, floating point numbers
   * will become float64 and arrays will become variable sized lists with
   * the data type inferred from the first element in the array.
   *
   * The schema must be specified if there are no records (e.g. to make
   * an empty table)
   */
  schema?: SchemaLike;

  /*
   * Mapping from vector column name to expected type
   *
   * Lance expects vector columns to be fixed size list arrays (i.e. tensors)
   * However, `makeArrowTable` will not infer this by default (it creates
   * variable size list arrays).  This field can be used to indicate that a column
   * should be treated as a vector column and converted to a fixed size list.
   *
   * The keys should be the names of the vector columns.  The value specifies the
   * expected data type of the vector columns.
   *
   * If `schema` is provided then this field is ignored.
   *
   * By default, the column named "vector" will be assumed to be a float32
   * vector column.
   */
  vectorColumns: Record<string, VectorColumnOptions> = {
    vector: new VectorColumnOptions(),
  };
  embeddings?: EmbeddingFunction<unknown>;
  embeddingFunction?: EmbeddingFunctionConfig;

  /**
   * If true then string columns will be encoded with dictionary encoding
   *
   * Set this to true if your string columns tend to repeat the same values
   * often.  For more precise control use the `schema` property to specify the
   * data type for individual columns.
   *
   * If `schema` is provided then this property is ignored.
   */
  dictionaryEncodeStrings: boolean = false;

  constructor(values?: Partial<MakeArrowTableOptions>) {
    Object.assign(this, values);
  }
}

/**
 * An enhanced version of the {@link makeTable} function from Apache Arrow
 * that supports nested fields and embeddings columns.
 *
 * (typically you do not need to call this function.  It will be called automatically
 * when creating a table or adding data to it)
 *
 * This function converts an array of Record<String, any> (row-major JS objects)
 * to an Arrow Table (a columnar structure)
 *
 * Note that it currently does not support nulls.
 *
 * If a schema is provided then it will be used to determine the resulting array
 * types.  Fields will also be reordered to fit the order defined by the schema.
 *
 * If a schema is not provided then the types will be inferred and the field order
 * will be controlled by the order of properties in the first record.  If a type
 * is inferred it will always be nullable.
 *
 * If the input is empty then a schema must be provided to create an empty table.
 *
 * When a schema is not specified then data types will be inferred.  The inference
 * rules are as follows:
 *
 *  - boolean => Bool
 *  - number => Float64
 *  - String => Utf8
 *  - Buffer => Binary
 *  - Record<String, any> => Struct
 *  - Array<any> => List
 * @example
 * import { fromTableToBuffer, makeArrowTable } from "../arrow";
 * import { Field, FixedSizeList, Float16, Float32, Int32, Schema } from "apache-arrow";
 *
 * const schema = new Schema([
 *   new Field("a", new Int32()),
 *   new Field("b", new Float32()),
 *   new Field("c", new FixedSizeList(3, new Field("item", new Float16()))),
 *  ]);
 *  const table = makeArrowTable([
 *    { a: 1, b: 2, c: [1, 2, 3] },
 *    { a: 4, b: 5, c: [4, 5, 6] },
 *    { a: 7, b: 8, c: [7, 8, 9] },
 *  ], { schema });
 * ```
 *
 * By default it assumes that the column named `vector` is a vector column
 * and it will be converted into a fixed size list array of type float32.
 * The `vectorColumns` option can be used to support other vector column
 * names and data types.
 *
 * ```ts
 *
 * const schema = new Schema([
    new Field("a", new Float64()),
    new Field("b", new Float64()),
    new Field(
      "vector",
      new FixedSizeList(3, new Field("item", new Float32()))
    ),
  ]);
  const table = makeArrowTable([
    { a: 1, b: 2, vector: [1, 2, 3] },
    { a: 4, b: 5, vector: [4, 5, 6] },
    { a: 7, b: 8, vector: [7, 8, 9] },
  ]);
  assert.deepEqual(table.schema, schema);
 * ```
 *
 * You can specify the vector column types and names using the options as well
 *
 * ```typescript
 *
 * const schema = new Schema([
    new Field('a', new Float64()),
    new Field('b', new Float64()),
    new Field('vec1', new FixedSizeList(3, new Field('item', new Float16()))),
    new Field('vec2', new FixedSizeList(3, new Field('item', new Float16())))
  ]);
 * const table = makeArrowTable([
    { a: 1, b: 2, vec1: [1, 2, 3], vec2: [2, 4, 6] },
    { a: 4, b: 5, vec1: [4, 5, 6], vec2: [8, 10, 12] },
    { a: 7, b: 8, vec1: [7, 8, 9], vec2: [14, 16, 18] }
  ], {
    vectorColumns: {
      vec1: { type: new Float16() },
      vec2: { type: new Float16() }
    }
  }
 * assert.deepEqual(table.schema, schema)
 * ```
 */
export function makeArrowTable(
  data: Array<Record<string, unknown>>,
  options?: Partial<MakeArrowTableOptions>,
  metadata?: Map<string, string>,
): ArrowTable {
  if (
    data.length === 0 &&
    (options?.schema === undefined || options?.schema === null)
  ) {
    throw new Error("At least one record or a schema needs to be provided");
  }

  const opt = new MakeArrowTableOptions(options !== undefined ? options : {});
  if (opt.schema !== undefined && opt.schema !== null) {
    opt.schema = sanitizeSchema(opt.schema);
    opt.schema = validateSchemaEmbeddings(
      opt.schema as Schema,
      data,
      options?.embeddingFunction,
    );
  }
  const columns: Record<string, Vector> = {};
  // TODO: sample dataset to find missing columns
  // Prefer the field ordering of the schema, if present
  const columnNames =
    opt.schema != null ? (opt.schema.names as string[]) : Object.keys(data[0]);
  for (const colName of columnNames) {
    if (
      data.length !== 0 &&
      !Object.prototype.hasOwnProperty.call(data[0], colName)
    ) {
      // The field is present in the schema, but not in the data, skip it
      continue;
    }
    // Extract a single column from the records (transpose from row-major to col-major)
    let values = data.map((datum) => datum[colName]);

    // By default (type === undefined) arrow will infer the type from the JS type
    let type;
    if (opt.schema !== undefined) {
      // If there is a schema provided, then use that for the type instead
      type = opt.schema?.fields.filter((f) => f.name === colName)[0]?.type;
      if (DataType.isInt(type) && type.bitWidth === 64) {
        // wrap in BigInt to avoid bug: https://github.com/apache/arrow/issues/40051
        values = values.map((v) => {
          if (v === null) {
            return v;
          }
          if (typeof v === "bigint") {
            return v;
          }
          if (typeof v === "number") {
            return BigInt(v);
          }
          throw new Error(
            `Expected BigInt or number for column ${colName}, got ${typeof v}`,
          );
        });
      }
    } else {
      // Otherwise, check to see if this column is one of the vector columns
      // defined by opt.vectorColumns and, if so, use the fixed size list type
      const vectorColumnOptions = opt.vectorColumns[colName];
      if (vectorColumnOptions !== undefined) {
        const firstNonNullValue = values.find((v) => v !== null);
        if (Array.isArray(firstNonNullValue)) {
          type = newVectorType(
            firstNonNullValue.length,
            vectorColumnOptions.type,
          );
        } else {
          throw new Error(
            `Column ${colName} is expected to be a vector column but first non-null value is not an array.  Could not determine size of vector column`,
          );
        }
      }
    }

    try {
      // Convert an Array of JS values to an arrow vector
      columns[colName] = makeVector(values, type, opt.dictionaryEncodeStrings);
    } catch (error: unknown) {
      // eslint-disable-next-line @typescript-eslint/restrict-template-expressions
      throw Error(`Could not convert column "${colName}" to Arrow: ${error}`);
    }
  }

  if (opt.schema != null) {
    // `new ArrowTable(columns)` infers a schema which may sometimes have
    // incorrect nullability (it assumes nullable=true always)
    //
    // `new ArrowTable(schema, columns)` will also fail because it will create a
    // batch with an inferred schema and then complain that the batch schema
    // does not match the provided schema.
    //
    // To work around this we first create a table with the wrong schema and
    // then patch the schema of the batches so we can use
    // `new ArrowTable(schema, batches)` which does not do any schema inference
    const firstTable = new ArrowTable(columns);
    const batchesFixed = firstTable.batches.map(
      (batch) => new RecordBatch(opt.schema as Schema, batch.data),
    );
    let schema: Schema;
    if (metadata !== undefined) {
      let schemaMetadata = opt.schema.metadata;
      if (schemaMetadata.size === 0) {
        schemaMetadata = metadata;
      } else {
        for (const [key, entry] of schemaMetadata.entries()) {
          schemaMetadata.set(key, entry);
        }
      }

      schema = new Schema(opt.schema.fields as Field[], schemaMetadata);
    } else {
      schema = opt.schema as Schema;
    }
    return new ArrowTable(schema, batchesFixed);
  }
  const tbl = new ArrowTable(columns);
  if (metadata !== undefined) {
    // biome-ignore lint/suspicious/noExplicitAny: <explanation>
    (<any>tbl.schema).metadata = metadata;
  }
  return tbl;
}

/**
 * Create an empty Arrow table with the provided schema
 */
export function makeEmptyTable(
  schema: SchemaLike,
  metadata?: Map<string, string>,
): ArrowTable {
  return makeArrowTable([], { schema }, metadata);
}

/**
 * Helper function to convert Array<Array<any>> to a variable sized list array
 */
// @ts-expect-error (Vector<unknown> is not assignable to Vector<any>)
function makeListVector(lists: unknown[][]): Vector<unknown> {
  if (lists.length === 0 || lists[0].length === 0) {
    throw Error("Cannot infer list vector from empty array or empty list");
  }
  const sampleList = lists[0];
  // biome-ignore lint/suspicious/noExplicitAny: skip
  let inferredType: any;
  try {
    const sampleVector = makeVector(sampleList);
    inferredType = sampleVector.type;
  } catch (error: unknown) {
    // eslint-disable-next-line @typescript-eslint/restrict-template-expressions
    throw Error(`Cannot infer list vector.  Cannot infer inner type: ${error}`);
  }

  const listBuilder = makeBuilder({
    type: new List(new Field("item", inferredType, true)),
  });
  for (const list of lists) {
    listBuilder.append(list);
  }
  return listBuilder.finish().toVector();
}

/** Helper function to convert an Array of JS values to an Arrow Vector */
function makeVector(
  values: unknown[],
  type?: DataType,
  stringAsDictionary?: boolean,
  // biome-ignore lint/suspicious/noExplicitAny: skip
): Vector<any> {
  if (type !== undefined) {
    // No need for inference, let Arrow create it
    return vectorFromArray(values, type);
  }
  if (values.length === 0) {
    throw Error(
      "makeVector requires at least one value or the type must be specfied",
    );
  }
  const sampleValue = values.find((val) => val !== null && val !== undefined);
  if (sampleValue === undefined) {
    throw Error(
      "makeVector cannot infer the type if all values are null or undefined",
    );
  }
  if (Array.isArray(sampleValue)) {
    // Default Arrow inference doesn't handle list types
    return makeListVector(values as unknown[][]);
  } else if (Buffer.isBuffer(sampleValue)) {
    // Default Arrow inference doesn't handle Buffer
    return vectorFromArray(values, new Binary());
  } else if (
    !(stringAsDictionary ?? false) &&
    (typeof sampleValue === "string" || sampleValue instanceof String)
  ) {
    // If the type is string then don't use Arrow's default inference unless dictionaries are requested
    // because it will always use dictionary encoding for strings
    return vectorFromArray(values, new Utf8());
  } else {
    // Convert a JS array of values to an arrow vector
    return vectorFromArray(values);
  }
}

/** Helper function to apply embeddings from metadata to an input table */
async function applyEmbeddingsFromMetadata(
  table: ArrowTable,
  schema: Schema,
): Promise<ArrowTable> {
  const registry = getRegistry();
  const functions = await registry.parseFunctions(schema.metadata);

  const columns = Object.fromEntries(
    table.schema.fields.map((field) => [
      field.name,
      table.getChild(field.name)!,
    ]),
  );

  for (const functionEntry of functions.values()) {
    const sourceColumn = columns[functionEntry.sourceColumn];
    const destColumn = functionEntry.vectorColumn ?? "vector";
    if (sourceColumn === undefined) {
      throw new Error(
        `Cannot apply embedding function because the source column '${functionEntry.sourceColumn}' was not present in the data`,
      );
    }
    if (columns[destColumn] !== undefined) {
      throw new Error(
        `Attempt to apply embeddings to table failed because column ${destColumn} already existed`,
      );
    }
    if (table.batches.length > 1) {
      throw new Error(
        "Internal error: `makeArrowTable` unexpectedly created a table with more than one batch",
      );
    }
    const values = sourceColumn.toArray();

    const vectors =
      await functionEntry.function.computeSourceEmbeddings(values);
    if (vectors.length !== values.length) {
      throw new Error(
        "Embedding function did not return an embedding for each input element",
      );
    }
    let destType: DataType;
    const dtype = schema.fields.find((f) => f.name === destColumn)!.type;
    if (isFixedSizeList(dtype)) {
      destType = sanitizeType(dtype);
    } else {
      throw new Error(
        "Expected FixedSizeList as datatype for vector field, instead got: " +
          dtype,
      );
    }
    const vector = makeVector(vectors, destType);
    columns[destColumn] = vector;
  }
  const newTable = new ArrowTable(columns);
  return alignTable(newTable, schema);
}

/** Helper function to apply embeddings to an input table */
async function applyEmbeddings<T>(
  table: ArrowTable,
  embeddings?: EmbeddingFunctionConfig,
  schema?: SchemaLike,
): Promise<ArrowTable> {
  if (schema !== undefined && schema !== null) {
    schema = sanitizeSchema(schema);
  }
  if (schema?.metadata.has("embedding_functions")) {
    return applyEmbeddingsFromMetadata(table, schema! as Schema);
  } else if (embeddings == null || embeddings === undefined) {
    return table;
  }

  // Convert from ArrowTable to Record<String, Vector>
  const colEntries = [...Array(table.numCols).keys()].map((_, idx) => {
    const name = table.schema.fields[idx].name;
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    const vec = table.getChildAt(idx)!;
    return [name, vec];
  });
  const newColumns = Object.fromEntries(colEntries);

  const sourceColumn = newColumns[embeddings.sourceColumn];
  const destColumn = embeddings.vectorColumn ?? "vector";
  const innerDestType =
    embeddings.function.embeddingDataType() ?? new Float32();
  if (sourceColumn === undefined) {
    throw new Error(
      `Cannot apply embedding function because the source column '${embeddings.sourceColumn}' was not present in the data`,
    );
  }

  if (table.numRows === 0) {
    if (Object.prototype.hasOwnProperty.call(newColumns, destColumn)) {
      // We have an empty table and it already has the embedding column so no work needs to be done
      // Note: we don't return an error like we did below because this is a common occurrence.  For example,
      // if we call convertToTable with 0 records and a schema that includes the embedding
      return table;
    }
    const dimensions = embeddings.function.ndims();
    if (dimensions !== undefined) {
      const destType = newVectorType(dimensions, innerDestType);
      newColumns[destColumn] = makeVector([], destType);
    } else if (schema != null) {
      const destField = schema.fields.find((f) => f.name === destColumn);
      if (destField != null) {
        newColumns[destColumn] = makeVector([], destField.type);
      } else {
        throw new Error(
          `Attempt to apply embeddings to an empty table failed because schema was missing embedding column '${destColumn}'`,
        );
      }
    } else {
      throw new Error(
        "Attempt to apply embeddings to an empty table when the embeddings function does not specify `embeddingDimension`",
      );
    }
  } else {
    if (Object.prototype.hasOwnProperty.call(newColumns, destColumn)) {
      throw new Error(
        `Attempt to apply embeddings to table failed because column ${destColumn} already existed`,
      );
    }
    if (table.batches.length > 1) {
      throw new Error(
        "Internal error: `makeArrowTable` unexpectedly created a table with more than one batch",
      );
    }
    const values = sourceColumn.toArray();
    const vectors = await embeddings.function.computeSourceEmbeddings(
      values as T[],
    );
    if (vectors.length !== values.length) {
      throw new Error(
        "Embedding function did not return an embedding for each input element",
      );
    }
    const destType = newVectorType(vectors[0].length, innerDestType);
    newColumns[destColumn] = makeVector(vectors, destType);
  }

  const newTable = new ArrowTable(newColumns);
  if (schema != null) {
    if (schema.fields.find((f) => f.name === destColumn) === undefined) {
      throw new Error(
        `When using embedding functions and specifying a schema the schema should include the embedding column but the column ${destColumn} was missing`,
      );
    }
    return alignTable(newTable, schema as Schema);
  }
  return newTable;
}

/**
 * Convert an Array of records into an Arrow Table, optionally applying an
 * embeddings function to it.
 *
 * This function calls `makeArrowTable` first to create the Arrow Table.
 * Any provided `makeTableOptions` (e.g. a schema) will be passed on to
 * that call.
 *
 * The embedding function will be passed a column of values (based on the
 * `sourceColumn` of the embedding function) and expects to receive back
 * number[][] which will be converted into a fixed size list column.  By
 * default this will be a fixed size list of Float32 but that can be
 * customized by the `embeddingDataType` property of the embedding function.
 *
 * If a schema is provided in `makeTableOptions` then it should include the
 * embedding columns.  If no schema is provded then embedding columns will
 * be placed at the end of the table, after all of the input columns.
 */
export async function convertToTable(
  data: Array<Record<string, unknown>>,
  embeddings?: EmbeddingFunctionConfig,
  makeTableOptions?: Partial<MakeArrowTableOptions>,
): Promise<ArrowTable> {
  const table = makeArrowTable(data, makeTableOptions);
  return await applyEmbeddings(table, embeddings, makeTableOptions?.schema);
}

/** Creates the Arrow Type for a Vector column with dimension `dim` */
export function newVectorType<T extends Float>(
  dim: number,
  innerType: unknown,
): FixedSizeList<T> {
  // in Lance we always default to have the elements nullable, so we need to set it to true
  // otherwise we often get schema mismatches because the stored data always has schema with nullable elements
  const children = new Field("item", <T>sanitizeType(innerType), true);
  return new FixedSizeList(dim, children);
}

/**
 * Serialize an Array of records into a buffer using the Arrow IPC File serialization
 *
 * This function will call `convertToTable` and pass on `embeddings` and `schema`
 *
 * `schema` is required if data is empty
 */
export async function fromRecordsToBuffer(
  data: Array<Record<string, unknown>>,
  embeddings?: EmbeddingFunctionConfig,
  schema?: Schema,
): Promise<Buffer> {
  if (schema !== undefined && schema !== null) {
    schema = sanitizeSchema(schema);
  }
  const table = await convertToTable(data, embeddings, { schema });
  const writer = RecordBatchFileWriter.writeAll(table);
  return Buffer.from(await writer.toUint8Array());
}

/**
 * Serialize an Array of records into a buffer using the Arrow IPC Stream serialization
 *
 * This function will call `convertToTable` and pass on `embeddings` and `schema`
 *
 * `schema` is required if data is empty
 */
export async function fromRecordsToStreamBuffer(
  data: Array<Record<string, unknown>>,
  embeddings?: EmbeddingFunctionConfig,
  schema?: Schema,
): Promise<Buffer> {
  if (schema !== undefined && schema !== null) {
    schema = sanitizeSchema(schema);
  }
  const table = await convertToTable(data, embeddings, { schema });
  const writer = RecordBatchStreamWriter.writeAll(table);
  return Buffer.from(await writer.toUint8Array());
}

/**
 * Serialize an Arrow Table into a buffer using the Arrow IPC File serialization
 *
 * This function will apply `embeddings` to the table in a manner similar to
 * `convertToTable`.
 *
 * `schema` is required if the table is empty
 */
export async function fromTableToBuffer(
  table: ArrowTable,
  embeddings?: EmbeddingFunctionConfig,
  schema?: SchemaLike,
): Promise<Buffer> {
  if (schema !== undefined && schema !== null) {
    schema = sanitizeSchema(schema);
  }
  const tableWithEmbeddings = await applyEmbeddings(table, embeddings, schema);
  const writer = RecordBatchFileWriter.writeAll(tableWithEmbeddings);
  return Buffer.from(await writer.toUint8Array());
}

/**
 * Serialize an Arrow Table into a buffer using the Arrow IPC File serialization
 *
 * This function will apply `embeddings` to the table in a manner similar to
 * `convertToTable`.
 *
 * `schema` is required if the table is empty
 */
export async function fromDataToBuffer(
  data: Data,
  embeddings?: EmbeddingFunctionConfig,
  schema?: Schema,
): Promise<Buffer> {
  if (schema !== undefined && schema !== null) {
    schema = sanitizeSchema(schema);
  }
  if (isArrowTable(data)) {
    return fromTableToBuffer(sanitizeTable(data), embeddings, schema);
  } else {
    const table = await convertToTable(data, embeddings, { schema });
    return fromTableToBuffer(table);
  }
}

/**
 * Serialize an Arrow Table into a buffer using the Arrow IPC Stream serialization
 *
 * This function will apply `embeddings` to the table in a manner similar to
 * `convertToTable`.
 *
 * `schema` is required if the table is empty
 */
export async function fromTableToStreamBuffer(
  table: ArrowTable,
  embeddings?: EmbeddingFunctionConfig,
  schema?: SchemaLike,
): Promise<Buffer> {
  const tableWithEmbeddings = await applyEmbeddings(table, embeddings, schema);
  const writer = RecordBatchStreamWriter.writeAll(tableWithEmbeddings);
  return Buffer.from(await writer.toUint8Array());
}

/**
 * Reorder the columns in `batch` so that they agree with the field order in `schema`
 */
function alignBatch(batch: RecordBatch, schema: Schema): RecordBatch {
  const alignedChildren = [];
  for (const field of schema.fields) {
    const indexInBatch = batch.schema.fields?.findIndex(
      (f) => f.name === field.name,
    );
    if (indexInBatch < 0) {
      throw new Error(
        `The column ${field.name} was not found in the Arrow Table`,
      );
    }
    alignedChildren.push(batch.data.children[indexInBatch]);
  }
  const newData = makeData({
    type: new Struct(schema.fields),
    length: batch.numRows,
    nullCount: batch.nullCount,
    children: alignedChildren,
  });
  return new RecordBatch(schema, newData);
}

/**
 * Reorder the columns in `table` so that they agree with the field order in `schema`
 */
function alignTable(table: ArrowTable, schema: Schema): ArrowTable {
  const alignedBatches = table.batches.map((batch) =>
    alignBatch(batch, schema),
  );
  return new ArrowTable(schema, alignedBatches);
}

/**
 * Create an empty table with the given schema
 */
export function createEmptyTable(schema: Schema): ArrowTable {
  return new ArrowTable(sanitizeSchema(schema));
}

function validateSchemaEmbeddings(
  schema: Schema,
  data: Array<Record<string, unknown>>,
  embeddings: EmbeddingFunctionConfig | undefined,
) {
  const fields = [];
  const missingEmbeddingFields = [];

  // First we check if the field is a `FixedSizeList`
  // Then we check if the data contains the field
  // if it does not, we add it to the list of missing embedding fields
  // Finally, we check if those missing embedding fields are `this._embeddings`
  // if they are not, we throw an error
  for (let field of schema.fields) {
    if (isFixedSizeList(field.type)) {
      field = sanitizeField(field);
      if (data.length !== 0 && data?.[0]?.[field.name] === undefined) {
        if (schema.metadata.has("embedding_functions")) {
          const embeddings = JSON.parse(
            schema.metadata.get("embedding_functions")!,
          );
          if (
            // biome-ignore lint/suspicious/noExplicitAny: we don't know the type of `f`
            embeddings.find((f: any) => f["vectorColumn"] === field.name) ===
            undefined
          ) {
            missingEmbeddingFields.push(field);
          }
        } else {
          missingEmbeddingFields.push(field);
        }
      } else {
        fields.push(field);
      }
    } else {
      fields.push(field);
    }
  }

  if (missingEmbeddingFields.length > 0 && embeddings === undefined) {
    throw new Error(
      `Table has embeddings: "${missingEmbeddingFields
        .map((f) => f.name)
        .join(",")}", but no embedding function was provided`,
    );
  }

  return new Schema(fields, schema.metadata);
}
