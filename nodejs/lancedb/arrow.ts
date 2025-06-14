// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import {
  Data as ArrowData,
  Table as ArrowTable,
  Binary,
  Bool,
  BufferType,
  DataType,
  DateUnit,
  Date_,
  Decimal,
  Dictionary,
  Duration,
  Field,
  FixedSizeBinary,
  FixedSizeList,
  Float,
  Float32,
  Float64,
  Int,
  Int32,
  Int64,
  LargeBinary,
  List,
  Null,
  Precision,
  RecordBatch,
  RecordBatchFileReader,
  RecordBatchFileWriter,
  RecordBatchStreamWriter,
  Schema,
  Struct,
  Timestamp,
  Type,
  Utf8,
  Vector,
  makeVector as arrowMakeVector,
  vectorFromArray as badVectorFromArray,
  makeBuilder,
  makeData,
  makeTable,
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

// biome-ignore lint/suspicious/noExplicitAny: skip
function vectorFromArray(data: any, type?: DataType) {
  // Workaround for: https://github.com/apache/arrow/issues/45862
  // If FSL type with float
  if (DataType.isFixedSizeList(type) && DataType.isFloat(type.valueType)) {
    const extendedData = [...data, new Array(type.listSize).fill(0.0)];
    const array = badVectorFromArray(extendedData, type);
    return array.slice(0, data.length);
  } else if (type === undefined) {
    return badVectorFromArray(data);
  } else {
    return badVectorFromArray(data, type);
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
 * If a schema is provided then it will be used to determine the resulting array
 * types.  Fields will also be reordered to fit the order defined by the schema.
 *
 * If a schema is not provided then the types will be inferred and the field order
 * will be controlled by the order of properties in the first record.  If a type
 * is inferred it will always be nullable.
 *
 * If not all fields are found in the data, then a subset of the schema will be
 * returned.
 *
 * If the input is empty then a schema must be provided to create an empty table.
 *
 * When a schema is not specified then data types will be inferred.  The inference
 * rules are as follows:
 *
 *  - boolean => Bool
 *  - number => Float64
 *  - bigint => Int64
 *  - String => Utf8
 *  - Buffer => Binary
 *  - Record<String, any> => Struct
 *  - Array<any> => List
 * @example
 * ```ts
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
 * const schema = new Schema([
 *   new Field("a", new Float64()),
 *   new Field("b", new Float64()),
 *   new Field(
 *     "vector",
 *     new FixedSizeList(3, new Field("item", new Float32()))
 *   ),
 * ]);
 * const table = makeArrowTable([
 *   { a: 1, b: 2, vector: [1, 2, 3] },
 *   { a: 4, b: 5, vector: [4, 5, 6] },
 *   { a: 7, b: 8, vector: [7, 8, 9] },
 * ]);
 * assert.deepEqual(table.schema, schema);
 * ```
 *
 * You can specify the vector column types and names using the options as well
 *
 * ```ts
 * const schema = new Schema([
 *   new Field('a', new Float64()),
 *   new Field('b', new Float64()),
 *   new Field('vec1', new FixedSizeList(3, new Field('item', new Float16()))),
 *   new Field('vec2', new FixedSizeList(3, new Field('item', new Float16())))
 * ]);
 * const table = makeArrowTable([
 *   { a: 1, b: 2, vec1: [1, 2, 3], vec2: [2, 4, 6] },
 *   { a: 4, b: 5, vec1: [4, 5, 6], vec2: [8, 10, 12] },
 *   { a: 7, b: 8, vec1: [7, 8, 9], vec2: [14, 16, 18] }
 * ], {
 *   vectorColumns: {
 *     vec1: { type: new Float16() },
 *     vec2: { type: new Float16() }
 *   }
 * }
 * assert.deepEqual(table.schema, schema)
 * ```
 */
export function makeArrowTable(
  data: Array<Record<string, unknown>>,
  options?: Partial<MakeArrowTableOptions>,
  metadata?: Map<string, string>,
): ArrowTable {
  const opt = new MakeArrowTableOptions(options !== undefined ? options : {});
  let schema: Schema | undefined = undefined;
  if (opt.schema !== undefined && opt.schema !== null) {
    schema = sanitizeSchema(opt.schema);
    schema = validateSchemaEmbeddings(
      schema as Schema,
      data,
      options?.embeddingFunction,
    );
  }

  let schemaMetadata = schema?.metadata || new Map<string, string>();
  if (metadata !== undefined) {
    schemaMetadata = new Map([...schemaMetadata, ...metadata]);
  }

  if (
    data.length === 0 &&
    (options?.schema === undefined || options?.schema === null)
  ) {
    throw new Error("At least one record or a schema needs to be provided");
  } else if (data.length === 0) {
    if (schema === undefined) {
      throw new Error("A schema must be provided if data is empty");
    } else {
      schema = new Schema(schema.fields, schemaMetadata);
      return new ArrowTable(schema);
    }
  }

  let inferredSchema = inferSchema(data, schema, opt);
  inferredSchema = new Schema(inferredSchema.fields, schemaMetadata);

  const finalColumns: Record<string, Vector> = {};
  for (const field of inferredSchema.fields) {
    finalColumns[field.name] = transposeData(data, field);
  }

  return new ArrowTable(inferredSchema, finalColumns);
}

function inferSchema(
  data: Array<Record<string, unknown>>,
  schema: Schema | undefined,
  opts: MakeArrowTableOptions,
): Schema {
  // We will collect all fields we see in the data.
  const pathTree = new PathTree<DataType>();

  for (const [rowI, row] of data.entries()) {
    for (const [path, value] of rowPathsAndValues(row)) {
      if (!pathTree.has(path)) {
        // First time seeing this field.
        if (schema !== undefined) {
          const field = getFieldForPath(schema, path);
          if (field === undefined) {
            throw new Error(
              `Found field not in schema: ${path.join(".")} at row ${rowI}`,
            );
          } else {
            pathTree.set(path, field.type);
          }
        } else {
          const inferredType = inferType(value, path, opts);
          if (inferredType === undefined) {
            throw new Error(`Failed to infer data type for field ${path.join(
              ".",
            )} at row ${rowI}. \
                             Consider providing an explicit schema.`);
          }
          pathTree.set(path, inferredType);
        }
      } else if (schema === undefined) {
        const currentType = pathTree.get(path);
        const newType = inferType(value, path, opts);
        if (currentType !== newType) {
          new Error(`Failed to infer schema for data. Previously inferred type \
                     ${currentType} but found ${newType} at row ${rowI}. Consider \
                     providing an explicit schema.`);
        }
      }
    }
  }

  if (schema === undefined) {
    function fieldsFromPathTree(pathTree: PathTree<DataType>): Field[] {
      const fields = [];
      for (const [name, value] of pathTree.map.entries()) {
        if (value instanceof PathTree) {
          const children = fieldsFromPathTree(value);
          fields.push(new Field(name, new Struct(children), true));
        } else {
          fields.push(new Field(name, value, true));
        }
      }
      return fields;
    }
    const fields = fieldsFromPathTree(pathTree);
    return new Schema(fields);
  } else {
    function takeMatchingFields(
      fields: Field[],
      pathTree: PathTree<DataType>,
    ): Field[] {
      const outFields = [];
      for (const field of fields) {
        if (pathTree.map.has(field.name)) {
          const value = pathTree.get([field.name]);
          if (value instanceof PathTree) {
            const struct = field.type as Struct;
            const children = takeMatchingFields(struct.children, value);
            outFields.push(
              new Field(field.name, new Struct(children), field.nullable),
            );
          } else {
            outFields.push(
              new Field(field.name, value as DataType, field.nullable),
            );
          }
        }
      }
      return outFields;
    }
    const fields = takeMatchingFields(schema.fields, pathTree);
    return new Schema(fields);
  }
}

function* rowPathsAndValues(
  row: Record<string, unknown>,
  basePath: string[] = [],
): Generator<[string[], unknown]> {
  for (const [key, value] of Object.entries(row)) {
    if (isObject(value)) {
      yield* rowPathsAndValues(value, [...basePath, key]);
    } else {
      yield [[...basePath, key], value];
    }
  }
}

function isObject(value: unknown): value is Record<string, unknown> {
  return (
    typeof value === "object" &&
    value !== null &&
    !Array.isArray(value) &&
    !(value instanceof RegExp) &&
    !(value instanceof Date) &&
    !(value instanceof Set) &&
    !(value instanceof Map) &&
    !(value instanceof Buffer)
  );
}

function getFieldForPath(schema: Schema, path: string[]): Field | undefined {
  let current: Field | Schema = schema;
  for (const key of path) {
    if (current instanceof Schema) {
      const field: Field | undefined = current.fields.find(
        (f) => f.name === key,
      );
      if (field === undefined) {
        return undefined;
      }
      current = field;
    } else if (current instanceof Field && DataType.isStruct(current.type)) {
      const struct: Struct = current.type;
      const field = struct.children.find((f) => f.name === key);
      if (field === undefined) {
        return undefined;
      }
      current = field;
    } else {
      return undefined;
    }
  }
  if (current instanceof Field) {
    return current;
  } else {
    return undefined;
  }
}

/**
 * Try to infer which Arrow type to use for a given value.
 *
 * May return undefined if the type cannot be inferred.
 */
function inferType(
  value: unknown,
  path: string[],
  opts: MakeArrowTableOptions,
): DataType | undefined {
  if (typeof value === "bigint") {
    return new Int64();
  } else if (typeof value === "number") {
    // Even if it's an integer, it's safer to assume Float64. Users can
    // always provide an explicit schema or use BigInt if they mean integer.
    return new Float64();
  } else if (typeof value === "string") {
    if (opts.dictionaryEncodeStrings) {
      return new Dictionary(new Utf8(), new Int32());
    } else {
      return new Utf8();
    }
  } else if (typeof value === "boolean") {
    return new Bool();
  } else if (value instanceof Buffer) {
    return new Binary();
  } else if (Array.isArray(value)) {
    if (value.length === 0) {
      return undefined; // Without any values we can't infer the type
    }
    if (path.length === 1 && Object.hasOwn(opts.vectorColumns, path[0])) {
      const floatType = sanitizeType(opts.vectorColumns[path[0]].type);
      return new FixedSizeList(
        value.length,
        new Field("item", floatType, true),
      );
    }
    const valueType = inferType(value[0], path, opts);
    if (valueType === undefined) {
      return undefined;
    }
    // Try to automatically detect embedding columns.
    if (valueType instanceof Float && path[path.length - 1] === "vector") {
      // We default to Float32 for vectors.
      const child = new Field("item", new Float32(), true);
      return new FixedSizeList(value.length, child);
    } else {
      const child = new Field("item", valueType, true);
      return new List(child);
    }
  } else {
    // TODO: timestamp
    return undefined;
  }
}

class PathTree<V> {
  map: Map<string, V | PathTree<V>>;

  constructor(entries?: [string[], V][]) {
    this.map = new Map();
    if (entries !== undefined) {
      for (const [path, value] of entries) {
        this.set(path, value);
      }
    }
  }
  has(path: string[]): boolean {
    let ref: PathTree<V> = this;
    for (const part of path) {
      if (!(ref instanceof PathTree) || !ref.map.has(part)) {
        return false;
      }
      ref = ref.map.get(part) as PathTree<V>;
    }
    return true;
  }
  get(path: string[]): V | undefined {
    let ref: PathTree<V> = this;
    for (const part of path) {
      if (!(ref instanceof PathTree) || !ref.map.has(part)) {
        return undefined;
      }
      ref = ref.map.get(part) as PathTree<V>;
    }
    return ref as V;
  }
  set(path: string[], value: V): void {
    let ref: PathTree<V> = this;
    for (const part of path.slice(0, path.length - 1)) {
      if (!ref.map.has(part)) {
        ref.map.set(part, new PathTree<V>());
      }
      ref = ref.map.get(part) as PathTree<V>;
    }
    ref.map.set(path[path.length - 1], value);
  }
}

function transposeData(
  data: Record<string, unknown>[],
  field: Field,
  path: string[] = [],
): Vector {
  if (field.type instanceof Struct) {
    const childFields = field.type.children;
    const fullPath = [...path, field.name];
    const childVectors = childFields.map((child) => {
      return transposeData(data, child, fullPath);
    });
    const structData = makeData({
      type: field.type,
      children: childVectors as unknown as ArrowData<DataType>[],
    });
    return arrowMakeVector(structData);
  } else {
    const valuesPath = [...path, field.name];
    const values = data.map((datum) => {
      let current: unknown = datum;
      for (const key of valuesPath) {
        if (current == null) {
          return null;
        }

        if (
          isObject(current) &&
          (Object.hasOwn(current, key) || key in current)
        ) {
          current = current[key];
        } else {
          return null;
        }
      }
      return current;
    });
    return makeVector(values, field.type);
  }
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
    if (type instanceof Int) {
      if (DataType.isInt(type) && type.bitWidth === 64) {
        // wrap in BigInt to avoid bug: https://github.com/apache/arrow/issues/40051
        values = values.map((v) => {
          if (v === null) {
            return v;
          } else if (typeof v === "bigint") {
            return v;
          } else if (typeof v === "number") {
            return BigInt(v);
          } else {
            return v;
          }
        });
      } else {
        // Similarly, bigint isn't supported for 16 or 32-bit ints.
        values = values.map((v) => {
          if (typeof v == "bigint") {
            return Number(v);
          } else {
            return v;
          }
        });
      }
    }
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

    // Check if destination column exists and handle accordingly
    if (columns[destColumn] !== undefined) {
      const existingColumn = columns[destColumn];
      // If the column exists but is all null, we can fill it with embeddings
      if (existingColumn.nullCount !== existingColumn.length) {
        // Column has non-null values, skip embedding application
        continue;
      }
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

  let schemaMetadata = schema?.metadata || new Map<string, string>();

  if (!(embeddings == null || embeddings === undefined)) {
    const registry = getRegistry();
    const embeddingMetadata = registry.getTableMetadata([embeddings]);
    schemaMetadata = new Map([...schemaMetadata, ...embeddingMetadata]);
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
    // Check if destination column exists and handle accordingly
    if (Object.prototype.hasOwnProperty.call(newColumns, destColumn)) {
      const existingColumn = newColumns[destColumn];
      // If the column exists but is all null, we can fill it with embeddings
      if (existingColumn.nullCount !== existingColumn.length) {
        // Column has non-null values, skip embedding application and return table as-is
        let newTable = new ArrowTable(newColumns);
        if (schema != null) {
          newTable = alignTable(newTable, schema as Schema);
        }
        return new ArrowTable(
          new Schema(newTable.schema.fields, schemaMetadata),
          newTable.batches,
        );
      }
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

  let newTable = new ArrowTable(newColumns);
  if (schema != null) {
    if (schema.fields.find((f) => f.name === destColumn) === undefined) {
      throw new Error(
        `When using embedding functions and specifying a schema the schema should include the embedding column but the column ${destColumn} was missing`,
      );
    }
    newTable = alignTable(newTable, schema as Schema);
  }

  newTable = new ArrowTable(
    new Schema(newTable.schema.fields, schemaMetadata),
    newTable.batches,
  );

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
 * Read a single record batch from a buffer.
 *
 * Returns null if the buffer does not contain a record batch
 */
export async function fromBufferToRecordBatch(
  data: Buffer,
): Promise<RecordBatch | null> {
  const iter = await RecordBatchFileReader.readAll(Buffer.from(data)).next()
    .value;
  const recordBatch = iter?.next().value;
  return recordBatch || null;
}

/**
 * Create a buffer containing a single record batch
 */
export async function fromRecordBatchToBuffer(
  batch: RecordBatch,
): Promise<Buffer> {
  const writer = new RecordBatchFileWriter().writeAll([batch]);
  return Buffer.from(await writer.toUint8Array());
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
): Schema {
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

interface JsonDataType {
  type: string;
  fields?: JsonField[];
  length?: number;
}

interface JsonField {
  name: string;
  type: JsonDataType;
  nullable: boolean;
  metadata: Map<string, string>;
}

// Matches format of https://github.com/lancedb/lance/blob/main/rust/lance/src/arrow/json.rs
export function dataTypeToJson(dataType: DataType): JsonDataType {
  switch (dataType.typeId) {
    // For primitives, matches https://github.com/lancedb/lance/blob/e12bb9eff2a52f753668d4b62c52e4d72b10d294/rust/lance-core/src/datatypes.rs#L185
    case Type.Null:
      return { type: "null" };
    case Type.Bool:
      return { type: "bool" };
    case Type.Int8:
      return { type: "int8" };
    case Type.Int16:
      return { type: "int16" };
    case Type.Int32:
      return { type: "int32" };
    case Type.Int64:
      return { type: "int64" };
    case Type.Uint8:
      return { type: "uint8" };
    case Type.Uint16:
      return { type: "uint16" };
    case Type.Uint32:
      return { type: "uint32" };
    case Type.Uint64:
      return { type: "uint64" };
    case Type.Int: {
      const bitWidth = (dataType as Int).bitWidth;
      const signed = (dataType as Int).isSigned;
      const prefix = signed ? "" : "u";
      return { type: `${prefix}int${bitWidth}` };
    }
    case Type.Float: {
      switch ((dataType as Float).precision) {
        case Precision.HALF:
          return { type: "halffloat" };
        case Precision.SINGLE:
          return { type: "float" };
        case Precision.DOUBLE:
          return { type: "double" };
      }
      throw Error("Unsupported float precision");
    }
    case Type.Float16:
      return { type: "halffloat" };
    case Type.Float32:
      return { type: "float" };
    case Type.Float64:
      return { type: "double" };
    case Type.Utf8:
      return { type: "string" };
    case Type.Binary:
      return { type: "binary" };
    case Type.LargeUtf8:
      return { type: "large_string" };
    case Type.LargeBinary:
      return { type: "large_binary" };
    case Type.List:
      return {
        type: "list",
        fields: [fieldToJson((dataType as List).children[0])],
      };
    case Type.FixedSizeList: {
      const fixedSizeList = dataType as FixedSizeList;
      return {
        type: "fixed_size_list",
        fields: [fieldToJson(fixedSizeList.children[0])],
        length: fixedSizeList.listSize,
      };
    }
    case Type.Struct:
      return {
        type: "struct",
        fields: (dataType as Struct).children.map(fieldToJson),
      };
    case Type.Date: {
      const unit = (dataType as Date_).unit;
      return {
        type: unit === DateUnit.DAY ? "date32:day" : "date64:ms",
      };
    }
    case Type.Timestamp: {
      const timestamp = dataType as Timestamp;
      const timezone = timestamp.timezone || "-";
      return {
        type: `timestamp:${timestamp.unit}:${timezone}`,
      };
    }
    case Type.Decimal: {
      const decimal = dataType as Decimal;
      return {
        type: `decimal:${decimal.bitWidth}:${decimal.precision}:${decimal.scale}`,
      };
    }
    case Type.Duration: {
      const duration = dataType as Duration;
      return { type: `duration:${duration.unit}` };
    }
    case Type.FixedSizeBinary: {
      const byteWidth = (dataType as FixedSizeBinary).byteWidth;
      return { type: `fixed_size_binary:${byteWidth}` };
    }
    case Type.Dictionary: {
      const dict = dataType as Dictionary;
      const indexType = dataTypeToJson(dict.indices);
      const valueType = dataTypeToJson(dict.valueType);
      return {
        type: `dict:${valueType.type}:${indexType.type}:false`,
      };
    }
  }
  throw new Error("Unsupported data type");
}

function fieldToJson(field: Field): JsonField {
  return {
    name: field.name,
    type: dataTypeToJson(field.type),
    nullable: field.nullable,
    metadata: field.metadata,
  };
}
