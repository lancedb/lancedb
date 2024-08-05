// Copyright 2023 LanceDB Developers.
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

// The utilities in this file help sanitize data from the user's arrow
// library into the types expected by vectordb's arrow library.  Node
// generally allows for mulitple versions of the same library (and sometimes
// even multiple copies of the same version) to be installed at the same
// time.  However, arrow-js uses instanceof which expected that the input
// comes from the exact same library instance.  This is not always the case
// and so we must sanitize the input to ensure that it is compatible.

import { BufferType, Data } from "apache-arrow";
import type { IntBitWidth, TKeys, TimeBitWidth } from "apache-arrow/type";
import {
  Binary,
  Bool,
  DataLike,
  DataType,
  DateDay,
  DateMillisecond,
  type DateUnit,
  Date_,
  Decimal,
  DenseUnion,
  Dictionary,
  Duration,
  DurationMicrosecond,
  DurationMillisecond,
  DurationNanosecond,
  DurationSecond,
  Field,
  FixedSizeBinary,
  FixedSizeList,
  Float,
  Float16,
  Float32,
  Float64,
  Int,
  Int8,
  Int16,
  Int32,
  Int64,
  Interval,
  IntervalDayTime,
  IntervalYearMonth,
  List,
  Map_,
  Null,
  type Precision,
  RecordBatch,
  RecordBatchLike,
  Schema,
  SchemaLike,
  SparseUnion,
  Struct,
  Table,
  TableLike,
  Time,
  TimeMicrosecond,
  TimeMillisecond,
  TimeNanosecond,
  TimeSecond,
  Timestamp,
  TimestampMicrosecond,
  TimestampMillisecond,
  TimestampNanosecond,
  TimestampSecond,
  Type,
  Uint8,
  Uint16,
  Uint32,
  Uint64,
  Union,
  Utf8,
} from "./arrow";

export function sanitizeMetadata(
  metadataLike?: unknown,
): Map<string, string> | undefined {
  if (metadataLike === undefined || metadataLike === null) {
    return undefined;
  }
  if (!(metadataLike instanceof Map)) {
    throw Error("Expected metadata, if present, to be a Map<string, string>");
  }
  for (const item of metadataLike) {
    if (!(typeof item[0] === "string" || !(typeof item[1] === "string"))) {
      throw Error(
        "Expected metadata, if present, to be a Map<string, string> but it had non-string keys or values",
      );
    }
  }
  return metadataLike as Map<string, string>;
}

export function sanitizeInt(typeLike: object) {
  if (
    !("bitWidth" in typeLike) ||
    typeof typeLike.bitWidth !== "number" ||
    !("isSigned" in typeLike) ||
    typeof typeLike.isSigned !== "boolean"
  ) {
    throw Error(
      "Expected an Int Type to have a `bitWidth` and `isSigned` property",
    );
  }
  return new Int(typeLike.isSigned, typeLike.bitWidth as IntBitWidth);
}

export function sanitizeFloat(typeLike: object) {
  if (!("precision" in typeLike) || typeof typeLike.precision !== "number") {
    throw Error("Expected a Float Type to have a `precision` property");
  }
  return new Float(typeLike.precision as Precision);
}

export function sanitizeDecimal(typeLike: object) {
  if (
    !("scale" in typeLike) ||
    typeof typeLike.scale !== "number" ||
    !("precision" in typeLike) ||
    typeof typeLike.precision !== "number" ||
    !("bitWidth" in typeLike) ||
    typeof typeLike.bitWidth !== "number"
  ) {
    throw Error(
      "Expected a Decimal Type to have `scale`, `precision`, and `bitWidth` properties",
    );
  }
  return new Decimal(typeLike.scale, typeLike.precision, typeLike.bitWidth);
}

export function sanitizeDate(typeLike: object) {
  if (!("unit" in typeLike) || typeof typeLike.unit !== "number") {
    throw Error("Expected a Date type to have a `unit` property");
  }
  return new Date_(typeLike.unit as DateUnit);
}

export function sanitizeTime(typeLike: object) {
  if (
    !("unit" in typeLike) ||
    typeof typeLike.unit !== "number" ||
    !("bitWidth" in typeLike) ||
    typeof typeLike.bitWidth !== "number"
  ) {
    throw Error(
      "Expected a Time type to have `unit` and `bitWidth` properties",
    );
  }
  return new Time(typeLike.unit, typeLike.bitWidth as TimeBitWidth);
}

export function sanitizeTimestamp(typeLike: object) {
  if (!("unit" in typeLike) || typeof typeLike.unit !== "number") {
    throw Error("Expected a Timestamp type to have a `unit` property");
  }
  let timezone = null;
  if ("timezone" in typeLike && typeof typeLike.timezone === "string") {
    timezone = typeLike.timezone;
  }
  return new Timestamp(typeLike.unit, timezone);
}

export function sanitizeTypedTimestamp(
  typeLike: object,
  // eslint-disable-next-line @typescript-eslint/naming-convention
  Datatype:
    | typeof TimestampNanosecond
    | typeof TimestampMicrosecond
    | typeof TimestampMillisecond
    | typeof TimestampSecond,
) {
  let timezone = null;
  if ("timezone" in typeLike && typeof typeLike.timezone === "string") {
    timezone = typeLike.timezone;
  }
  return new Datatype(timezone);
}

export function sanitizeInterval(typeLike: object) {
  if (!("unit" in typeLike) || typeof typeLike.unit !== "number") {
    throw Error("Expected an Interval type to have a `unit` property");
  }
  return new Interval(typeLike.unit);
}

export function sanitizeList(typeLike: object) {
  if (!("children" in typeLike) || !Array.isArray(typeLike.children)) {
    throw Error(
      "Expected a List type to have an array-like `children` property",
    );
  }
  if (typeLike.children.length !== 1) {
    throw Error("Expected a List type to have exactly one child");
  }
  return new List(sanitizeField(typeLike.children[0]));
}

export function sanitizeStruct(typeLike: object) {
  if (!("children" in typeLike) || !Array.isArray(typeLike.children)) {
    throw Error(
      "Expected a Struct type to have an array-like `children` property",
    );
  }
  return new Struct(typeLike.children.map((child) => sanitizeField(child)));
}

export function sanitizeUnion(typeLike: object) {
  if (
    !("typeIds" in typeLike) ||
    !("mode" in typeLike) ||
    typeof typeLike.mode !== "number"
  ) {
    throw Error(
      "Expected a Union type to have `typeIds` and `mode` properties",
    );
  }
  if (!("children" in typeLike) || !Array.isArray(typeLike.children)) {
    throw Error(
      "Expected a Union type to have an array-like `children` property",
    );
  }

  return new Union(
    typeLike.mode,
    // biome-ignore lint/suspicious/noExplicitAny: skip
    typeLike.typeIds as any,
    typeLike.children.map((child) => sanitizeField(child)),
  );
}

export function sanitizeTypedUnion(
  typeLike: object,
  // eslint-disable-next-line @typescript-eslint/naming-convention
  UnionType: typeof DenseUnion | typeof SparseUnion,
) {
  if (!("typeIds" in typeLike)) {
    throw Error(
      "Expected a DenseUnion/SparseUnion type to have a `typeIds` property",
    );
  }
  if (!("children" in typeLike) || !Array.isArray(typeLike.children)) {
    throw Error(
      "Expected a DenseUnion/SparseUnion type to have an array-like `children` property",
    );
  }

  return new UnionType(
    typeLike.typeIds as Int32Array | number[],
    typeLike.children.map((child) => sanitizeField(child)),
  );
}

export function sanitizeFixedSizeBinary(typeLike: object) {
  if (!("byteWidth" in typeLike) || typeof typeLike.byteWidth !== "number") {
    throw Error(
      "Expected a FixedSizeBinary type to have a `byteWidth` property",
    );
  }
  return new FixedSizeBinary(typeLike.byteWidth);
}

export function sanitizeFixedSizeList(typeLike: object) {
  if (!("listSize" in typeLike) || typeof typeLike.listSize !== "number") {
    throw Error("Expected a FixedSizeList type to have a `listSize` property");
  }
  if (!("children" in typeLike) || !Array.isArray(typeLike.children)) {
    throw Error(
      "Expected a FixedSizeList type to have an array-like `children` property",
    );
  }
  if (typeLike.children.length !== 1) {
    throw Error("Expected a FixedSizeList type to have exactly one child");
  }
  return new FixedSizeList(
    typeLike.listSize,
    sanitizeField(typeLike.children[0]),
  );
}

export function sanitizeMap(typeLike: object) {
  if (!("children" in typeLike) || !Array.isArray(typeLike.children)) {
    throw Error(
      "Expected a Map type to have an array-like `children` property",
    );
  }
  if (!("keysSorted" in typeLike) || typeof typeLike.keysSorted !== "boolean") {
    throw Error("Expected a Map type to have a `keysSorted` property");
  }

  return new Map_(
    // biome-ignore lint/suspicious/noExplicitAny: skip
    typeLike.children.map((field) => sanitizeField(field)) as any,
    typeLike.keysSorted,
  );
}

export function sanitizeDuration(typeLike: object) {
  if (!("unit" in typeLike) || typeof typeLike.unit !== "number") {
    throw Error("Expected a Duration type to have a `unit` property");
  }
  return new Duration(typeLike.unit);
}

export function sanitizeDictionary(typeLike: object) {
  if (!("id" in typeLike) || typeof typeLike.id !== "number") {
    throw Error("Expected a Dictionary type to have an `id` property");
  }
  if (!("indices" in typeLike) || typeof typeLike.indices !== "object") {
    throw Error("Expected a Dictionary type to have an `indices` property");
  }
  if (!("dictionary" in typeLike) || typeof typeLike.dictionary !== "object") {
    throw Error("Expected a Dictionary type to have an `dictionary` property");
  }
  if (!("isOrdered" in typeLike) || typeof typeLike.isOrdered !== "boolean") {
    throw Error("Expected a Dictionary type to have an `isOrdered` property");
  }
  return new Dictionary(
    sanitizeType(typeLike.dictionary),
    sanitizeType(typeLike.indices) as TKeys,
    typeLike.id,
    typeLike.isOrdered,
  );
}

// biome-ignore lint/suspicious/noExplicitAny: skip
export function sanitizeType(typeLike: unknown): DataType<any> {
  if (typeof typeLike !== "object" || typeLike === null) {
    throw Error("Expected a Type but object was null/undefined");
  }
  if (
    !("typeId" in typeLike) ||
    !(
      typeof typeLike.typeId !== "function" ||
      typeof typeLike.typeId !== "number"
    )
  ) {
    throw Error("Expected a Type to have a typeId property");
  }
  let typeId: Type;
  if (typeof typeLike.typeId === "function") {
    typeId = (typeLike.typeId as () => unknown)() as Type;
  } else if (typeof typeLike.typeId === "number") {
    typeId = typeLike.typeId as Type;
  } else {
    throw Error("Type's typeId property was not a function or number");
  }

  switch (typeId) {
    case Type.NONE:
      throw Error("Received a Type with a typeId of NONE");
    case Type.Null:
      return new Null();
    case Type.Int:
      return sanitizeInt(typeLike);
    case Type.Float:
      return sanitizeFloat(typeLike);
    case Type.Binary:
      return new Binary();
    case Type.Utf8:
      return new Utf8();
    case Type.Bool:
      return new Bool();
    case Type.Decimal:
      return sanitizeDecimal(typeLike);
    case Type.Date:
      return sanitizeDate(typeLike);
    case Type.Time:
      return sanitizeTime(typeLike);
    case Type.Timestamp:
      return sanitizeTimestamp(typeLike);
    case Type.Interval:
      return sanitizeInterval(typeLike);
    case Type.List:
      return sanitizeList(typeLike);
    case Type.Struct:
      return sanitizeStruct(typeLike);
    case Type.Union:
      return sanitizeUnion(typeLike);
    case Type.FixedSizeBinary:
      return sanitizeFixedSizeBinary(typeLike);
    case Type.FixedSizeList:
      return sanitizeFixedSizeList(typeLike);
    case Type.Map:
      return sanitizeMap(typeLike);
    case Type.Duration:
      return sanitizeDuration(typeLike);
    case Type.Dictionary:
      return sanitizeDictionary(typeLike);
    case Type.Int8:
      return new Int8();
    case Type.Int16:
      return new Int16();
    case Type.Int32:
      return new Int32();
    case Type.Int64:
      return new Int64();
    case Type.Uint8:
      return new Uint8();
    case Type.Uint16:
      return new Uint16();
    case Type.Uint32:
      return new Uint32();
    case Type.Uint64:
      return new Uint64();
    case Type.Float16:
      return new Float16();
    case Type.Float32:
      return new Float32();
    case Type.Float64:
      return new Float64();
    case Type.DateMillisecond:
      return new DateMillisecond();
    case Type.DateDay:
      return new DateDay();
    case Type.TimeNanosecond:
      return new TimeNanosecond();
    case Type.TimeMicrosecond:
      return new TimeMicrosecond();
    case Type.TimeMillisecond:
      return new TimeMillisecond();
    case Type.TimeSecond:
      return new TimeSecond();
    case Type.TimestampNanosecond:
      return sanitizeTypedTimestamp(typeLike, TimestampNanosecond);
    case Type.TimestampMicrosecond:
      return sanitizeTypedTimestamp(typeLike, TimestampMicrosecond);
    case Type.TimestampMillisecond:
      return sanitizeTypedTimestamp(typeLike, TimestampMillisecond);
    case Type.TimestampSecond:
      return sanitizeTypedTimestamp(typeLike, TimestampSecond);
    case Type.DenseUnion:
      return sanitizeTypedUnion(typeLike, DenseUnion);
    case Type.SparseUnion:
      return sanitizeTypedUnion(typeLike, SparseUnion);
    case Type.IntervalDayTime:
      return new IntervalDayTime();
    case Type.IntervalYearMonth:
      return new IntervalYearMonth();
    case Type.DurationNanosecond:
      return new DurationNanosecond();
    case Type.DurationMicrosecond:
      return new DurationMicrosecond();
    case Type.DurationMillisecond:
      return new DurationMillisecond();
    case Type.DurationSecond:
      return new DurationSecond();
    default:
      throw new Error("Unrecoginized type id in schema: " + typeId);
  }
}

export function sanitizeField(fieldLike: unknown): Field {
  if (fieldLike instanceof Field) {
    return fieldLike;
  }
  if (typeof fieldLike !== "object" || fieldLike === null) {
    throw Error("Expected a Field but object was null/undefined");
  }
  if (
    !("type" in fieldLike) ||
    !("name" in fieldLike) ||
    !("nullable" in fieldLike)
  ) {
    throw Error(
      "The field passed in is missing a `type`/`name`/`nullable` property",
    );
  }
  const type = sanitizeType(fieldLike.type);
  const name = fieldLike.name;
  if (!(typeof name === "string")) {
    throw Error("The field passed in had a non-string `name` property");
  }
  const nullable = fieldLike.nullable;
  if (!(typeof nullable === "boolean")) {
    throw Error("The field passed in had a non-boolean `nullable` property");
  }
  let metadata;
  if ("metadata" in fieldLike) {
    metadata = sanitizeMetadata(fieldLike.metadata);
  }
  return new Field(name, type, nullable, metadata);
}

/**
 * Convert something schemaLike into a Schema instance
 *
 * This method is often needed even when the caller is using a Schema
 * instance because they might be using a different instance of apache-arrow
 * than lancedb is using.
 */
export function sanitizeSchema(schemaLike: SchemaLike): Schema {
  if (schemaLike instanceof Schema) {
    return schemaLike;
  }
  if (typeof schemaLike !== "object" || schemaLike === null) {
    throw Error("Expected a Schema but object was null/undefined");
  }
  if (!("fields" in schemaLike)) {
    throw Error(
      "The schema passed in does not appear to be a schema (no 'fields' property)",
    );
  }
  let metadata;
  if ("metadata" in schemaLike) {
    metadata = sanitizeMetadata(schemaLike.metadata);
  }
  if (!Array.isArray(schemaLike.fields)) {
    throw Error(
      "The schema passed in had a 'fields' property but it was not an array",
    );
  }
  const sanitizedFields = schemaLike.fields.map((field) =>
    sanitizeField(field),
  );
  return new Schema(sanitizedFields, metadata);
}

export function sanitizeTable(tableLike: TableLike): Table {
  if (tableLike instanceof Table) {
    return tableLike;
  }
  if (typeof tableLike !== "object" || tableLike === null) {
    throw Error("Expected a Table but object was null/undefined");
  }
  if (!("schema" in tableLike)) {
    throw Error(
      "The table passed in does not appear to be a table (no 'schema' property)",
    );
  }
  if (!("batches" in tableLike)) {
    throw Error(
      "The table passed in does not appear to be a table (no 'columns' property)",
    );
  }
  const schema = sanitizeSchema(tableLike.schema);

  const batches = tableLike.batches.map(sanitizeRecordBatch);
  return new Table(schema, batches);
}

function sanitizeRecordBatch(batchLike: RecordBatchLike): RecordBatch {
  if (batchLike instanceof RecordBatch) {
    return batchLike;
  }
  if (typeof batchLike !== "object" || batchLike === null) {
    throw Error("Expected a RecordBatch but object was null/undefined");
  }
  if (!("schema" in batchLike)) {
    throw Error(
      "The record batch passed in does not appear to be a record batch (no 'schema' property)",
    );
  }
  if (!("data" in batchLike)) {
    throw Error(
      "The record batch passed in does not appear to be a record batch (no 'data' property)",
    );
  }
  const schema = sanitizeSchema(batchLike.schema);
  const data = sanitizeData(batchLike.data);
  return new RecordBatch(schema, data);
}
function sanitizeData(
  dataLike: DataLike,
  // biome-ignore lint/suspicious/noExplicitAny: <explanation>
): import("apache-arrow").Data<Struct<any>> {
  if (dataLike instanceof Data) {
    return dataLike;
  }
  return new Data(
    dataLike.type,
    dataLike.offset,
    dataLike.length,
    dataLike.nullCount,
    {
      [BufferType.OFFSET]: dataLike.valueOffsets,
      [BufferType.DATA]: dataLike.values,
      [BufferType.VALIDITY]: dataLike.nullBitmap,
      [BufferType.TYPE]: dataLike.typeIds,
    },
  );
}
