// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import {
  Binary,
  Bool,
  DataType,
  Dictionary,
  Field,
  FixedSizeList,
  Float32,
  Float64,
  Int32,
  Int64,
  List,
  Schema,
  Struct,
  Utf8,
  util as arrowUtil,
} from "apache-arrow";
import { typedArrayToArrowType } from "./arrow_type";
import { sanitizeType } from "./sanitize";

type InferenceOptions = {
  dictionaryEncodeStrings: boolean;
  vectorColumns: Record<string, { type: unknown }>;
};

/**
 * Infer the Arrow schema represented by a set of records.
 *
 * This is the intentionally small interface to schema inference. The stateful
 * details of combining partial type evidence are encapsulated below so callers
 * only need to provide records, an optional schema, and inference options.
 */
export function inferSchema(
  data: Array<Record<string, unknown>>,
  schema: Schema | undefined,
  options: InferenceOptions,
): Schema {
  return new SchemaInferrer(schema, options).infer(data);
}

class SchemaInferrer {
  private readonly fields = new FieldTree();

  constructor(
    private readonly providedSchema: Schema | undefined,
    private readonly options: InferenceOptions,
  ) {}

  infer(data: Array<Record<string, unknown>>): Schema {
    for (const [row, record] of data.entries()) {
      for (const [path, value] of recordPathsAndValues(record)) {
        this.observe(path, value, row);
      }
    }

    return this.providedSchema === undefined
      ? new Schema(fieldsFromTree(this.fields))
      : new Schema(matchingFields(this.providedSchema.fields, this.fields));
  }

  private observe(path: string[], value: unknown, row: number): void {
    const current = this.fields.get(path);
    if (current === undefined) {
      this.addField(path, value, row);
    } else if (this.providedSchema === undefined) {
      this.updateInferredField(path, value, row, current);
    }
  }

  private addField(path: string[], value: unknown, row: number): void {
    if (this.providedSchema !== undefined) {
      this.addSchemaField(this.providedSchema, path, row);
      return;
    }

    const evidence =
      this.inferType(value, path) ?? DeferredTypeEvidence.from(value, row);
    if (evidence === undefined) {
      throw typeInferenceError(path, row);
    }

    const conflict = this.fields.set(
      path,
      evidence,
      (existing) =>
        existing instanceof DeferredTypeEvidence && existing.isOnlyNulls(),
    );
    if (conflict !== undefined) {
      throw branchConflictError(conflict, row, "Struct");
    }
  }

  private addSchemaField(schema: Schema, path: string[], row: number): void {
    const field = fieldAtPath(schema, path);
    if (field === undefined) {
      throw new Error(
        `Found field not in schema: ${path.join(".")} at row ${row}`,
      );
    }

    const conflict = this.fields.set(path, field.type);
    if (conflict !== undefined) {
      throw branchConflictError(conflict, row, "Struct");
    }
  }

  private updateInferredField(
    path: string[],
    value: unknown,
    row: number,
    current: FieldNode,
  ): void {
    const newType = this.inferType(value, path);
    const deferred = DeferredTypeEvidence.from(value, row);

    if (current instanceof FieldTree) {
      if (deferred?.isOnlyNulls()) {
        return;
      }
      throw schemaInferenceError(
        path,
        row,
        "Struct",
        describeEvidence(newType ?? deferred),
      );
    }

    if (current instanceof DeferredTypeEvidence) {
      this.resolveDeferredField(path, row, current, newType, deferred);
      return;
    }

    if (newType !== undefined) {
      if (!inferredTypesEqual(current, newType)) {
        throw schemaInferenceError(
          path,
          row,
          describeEvidence(current),
          describeEvidence(newType),
        );
      }
      return;
    }

    if (deferred === undefined || !deferred.matches(current)) {
      throw schemaInferenceError(
        path,
        row,
        describeEvidence(current),
        describeEvidence(deferred),
      );
    }
  }

  private resolveDeferredField(
    path: string[],
    row: number,
    current: DeferredTypeEvidence,
    newType: DataType | undefined,
    deferred: DeferredTypeEvidence | undefined,
  ): void {
    if (newType !== undefined) {
      if (!current.matches(newType)) {
        throw schemaInferenceError(
          path,
          row,
          current.describe(),
          describeEvidence(newType),
        );
      }
      this.fields.set(path, newType);
      return;
    }

    if (deferred !== undefined) {
      this.fields.set(path, current.merge(deferred));
      return;
    }

    throw schemaInferenceError(
      path,
      row,
      current.describe(),
      describeEvidence(newType),
    );
  }

  private inferType(value: unknown, path: string[]): DataType | undefined {
    if (typeof value === "bigint") {
      return new Int64();
    }
    if (typeof value === "number") {
      return new Float64();
    }
    if (typeof value === "string") {
      return this.options.dictionaryEncodeStrings
        ? new Dictionary(new Utf8(), new Int32())
        : new Utf8();
    }
    if (typeof value === "boolean") {
      return new Bool();
    }
    if (value instanceof Buffer) {
      return new Binary();
    }
    if (ArrayBuffer.isView(value) && !(value instanceof DataView)) {
      const typedArray = typedArrayToArrowType(value);
      return typedArray === undefined
        ? undefined
        : new FixedSizeList(
            typedArray.length,
            new Field("item", typedArray.elementType, true),
          );
    }
    if (!Array.isArray(value) || value.length === 0) {
      return undefined;
    }

    const configuredVector =
      path.length === 1 ? this.options.vectorColumns[path[0]] : undefined;
    if (configuredVector !== undefined) {
      return new FixedSizeList(
        value.length,
        new Field("item", sanitizeType(configuredVector.type), true),
      );
    }

    const itemType = this.inferArrayItemType(value, path);
    if (itemType === undefined) {
      return undefined;
    }

    return nameSuggestsVectorColumn(path[path.length - 1])
      ? new FixedSizeList(value.length, new Field("item", new Float32(), true))
      : new List(new Field("item", itemType, true));
  }

  private inferArrayItemType(
    values: unknown[],
    path: string[],
  ): DataType | undefined {
    let itemType: DataType | undefined;
    const deferredItems: unknown[] = [];

    for (const value of values) {
      const candidate = this.inferType(value, path);
      if (candidate === undefined) {
        if (!isDeferredValue(value)) {
          return undefined;
        }
        deferredItems.push(value);
      } else if (itemType === undefined) {
        itemType = candidate;
      } else if (!inferredTypesEqual(itemType, candidate)) {
        return undefined;
      }
    }

    if (itemType === undefined) {
      return undefined;
    }
    return deferredItems.every((value) =>
      deferredValueMatchesType(value, itemType),
    )
      ? itemType
      : undefined;
  }
}

/** Nulls and empty/all-null lists that do not determine a type by themselves. */
class DeferredTypeEvidence {
  private constructor(
    private readonly values: Array<{ value: unknown; row: number }>,
  ) {}

  static from(value: unknown, row: number): DeferredTypeEvidence | undefined {
    return isDeferredValue(value)
      ? new DeferredTypeEvidence([{ value, row }])
      : undefined;
  }

  isOnlyNulls(): boolean {
    return this.values.every(({ value }) => value == null);
  }

  matches(type: DataType): boolean {
    return this.values.every(({ value }) =>
      deferredValueMatchesType(value, type),
    );
  }

  merge(other: DeferredTypeEvidence): DeferredTypeEvidence {
    return new DeferredTypeEvidence([...this.values, ...other.values]);
  }

  describe(): string {
    const list = this.values.find(({ value }) => Array.isArray(value));
    return list === undefined
      ? "null"
      : `List[${(list.value as unknown[]).length}]`;
  }

  firstRow(): number {
    return this.values[0].row;
  }
}

type FieldNode = DataType | DeferredTypeEvidence | FieldTree;
type LeafNode = Exclude<FieldNode, FieldTree>;
type FieldConflict = { path: string[]; value: FieldNode };

/** Nested field state, kept separate from Arrow's eventual Struct types. */
class FieldTree {
  private readonly children = new Map<string, FieldNode>();

  get(path: string[]): FieldNode | undefined {
    let current: FieldNode = this;
    for (const part of path) {
      if (!(current instanceof FieldTree)) {
        return undefined;
      }
      const child = current.children.get(part);
      if (child === undefined) {
        return undefined;
      }
      current = child;
    }
    return current;
  }

  set(
    path: string[],
    value: LeafNode,
    canReplaceLeaf: (value: LeafNode) => boolean = () => false,
  ): FieldConflict | undefined {
    let branch: FieldTree = this;
    for (const [index, part] of path.slice(0, -1).entries()) {
      const child = branch.children.get(part);
      if (child === undefined || (isLeaf(child) && canReplaceLeaf(child))) {
        const nextBranch = new FieldTree();
        branch.children.set(part, nextBranch);
        branch = nextBranch;
      } else if (child instanceof FieldTree) {
        branch = child;
      } else {
        return { path: path.slice(0, index + 1), value: child };
      }
    }

    const name = path[path.length - 1];
    const current = branch.children.get(name);
    if (current instanceof FieldTree) {
      return { path, value: current };
    }
    branch.children.set(name, value);
    return undefined;
  }

  entries(): IterableIterator<[string, FieldNode]> {
    return this.children.entries();
  }

  has(name: string): boolean {
    return this.children.has(name);
  }
}

function isLeaf(value: FieldNode): value is LeafNode {
  return !(value instanceof FieldTree);
}

function fieldsFromTree(tree: FieldTree, path: string[] = []): Field[] {
  const fields: Field[] = [];
  for (const [name, value] of tree.entries()) {
    if (value instanceof FieldTree) {
      fields.push(
        new Field(
          name,
          new Struct(fieldsFromTree(value, [...path, name])),
          true,
        ),
      );
    } else if (value instanceof DeferredTypeEvidence) {
      throw typeInferenceError([...path, name], value.firstRow());
    } else {
      fields.push(new Field(name, value, true));
    }
  }
  return fields;
}

function matchingFields(fields: Field[], tree: FieldTree): Field[] {
  const matches: Field[] = [];
  for (const field of fields) {
    if (!tree.has(field.name)) {
      continue;
    }
    const value = tree.get([field.name]);
    if (value instanceof FieldTree) {
      const struct = field.type as Struct;
      matches.push(
        new Field(
          field.name,
          new Struct(matchingFields(struct.children, value)),
          field.nullable,
        ),
      );
    } else {
      matches.push(new Field(field.name, value as DataType, field.nullable));
    }
  }
  return matches;
}

function* recordPathsAndValues(
  record: Record<string, unknown>,
  path: string[] = [],
): Generator<[string[], unknown]> {
  for (const [name, value] of Object.entries(record)) {
    if (isRecord(value)) {
      yield* recordPathsAndValues(value, [...path, name]);
    } else if (value !== undefined) {
      yield [[...path, name], value];
    }
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return (
    typeof value === "object" &&
    value !== null &&
    !Array.isArray(value) &&
    !(value instanceof RegExp) &&
    !(value instanceof Date) &&
    !(value instanceof Set) &&
    !(value instanceof Map) &&
    !(value instanceof Buffer) &&
    !ArrayBuffer.isView(value)
  );
}

function fieldAtPath(schema: Schema, path: string[]): Field | undefined {
  let fields = schema.fields;
  let field: Field | undefined;
  for (const [index, name] of path.entries()) {
    field = fields.find((candidate) => candidate.name === name);
    if (field === undefined || index === path.length - 1) {
      return field;
    }
    if (!DataType.isStruct(field.type)) {
      return undefined;
    }
    fields = field.type.children;
  }
  return field;
}

function isDeferredValue(value: unknown): boolean {
  return (
    value == null || (Array.isArray(value) && value.every(isDeferredValue))
  );
}

function deferredValueMatchesType(value: unknown, type: DataType): boolean {
  if (value == null) {
    return true;
  }
  if (!Array.isArray(value)) {
    return false;
  }
  if (DataType.isList(type)) {
    return value.every((item) =>
      deferredValueMatchesType(item, type.valueType),
    );
  }
  if (DataType.isFixedSizeList(type)) {
    return (
      value.length === type.listSize &&
      value.every((item) => deferredValueMatchesType(item, type.valueType))
    );
  }
  return false;
}

function inferredTypesEqual(current: DataType, candidate: DataType): boolean {
  if (DataType.isDictionary(current)) {
    return (
      DataType.isDictionary(candidate) &&
      current.isOrdered === candidate.isOrdered &&
      inferredTypesEqual(current.indices, candidate.indices) &&
      inferredTypesEqual(current.dictionary, candidate.dictionary)
    );
  }
  if (DataType.isList(current)) {
    return (
      DataType.isList(candidate) &&
      current.valueField.name === candidate.valueField.name &&
      current.valueField.nullable === candidate.valueField.nullable &&
      inferredTypesEqual(current.valueType, candidate.valueType)
    );
  }
  if (DataType.isFixedSizeList(current)) {
    return (
      DataType.isFixedSizeList(candidate) &&
      current.listSize === candidate.listSize &&
      current.valueField.name === candidate.valueField.name &&
      current.valueField.nullable === candidate.valueField.nullable &&
      inferredTypesEqual(current.valueType, candidate.valueType)
    );
  }
  return arrowUtil.compareTypes(current, candidate);
}

function describeEvidence(
  evidence: DataType | DeferredTypeEvidence | undefined,
): string {
  if (evidence === undefined) {
    return "an unsupported value";
  }
  return evidence instanceof DeferredTypeEvidence
    ? evidence.describe()
    : evidence.toString();
}

function branchConflictError(
  conflict: FieldConflict,
  row: number,
  candidate: string,
): Error {
  return schemaInferenceError(
    conflict.path,
    row,
    conflict.value instanceof FieldTree
      ? "Struct"
      : describeEvidence(conflict.value),
    candidate,
  );
}

function schemaInferenceError(
  path: string[],
  row: number,
  currentType: string,
  newType: string,
): Error {
  return new Error(
    `Failed to infer schema for data. Previously inferred type ${currentType} ` +
      `but found ${newType} for field ${path.join(".")} at row ${row}. ` +
      "Consider providing an explicit schema.",
  );
}

function typeInferenceError(path: string[], row: number): Error {
  return new Error(
    `Failed to infer data type for field ${path.join(".")} at row ${row}. ` +
      "Consider providing an explicit schema.",
  );
}

function nameSuggestsVectorColumn(name: string): boolean {
  const normalized = name.toLowerCase();
  return normalized.includes("vector") || normalized.includes("embedding");
}
