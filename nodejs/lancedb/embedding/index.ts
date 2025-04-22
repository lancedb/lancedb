// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import { Field, Schema } from "../arrow";
import { sanitizeType } from "../sanitize";
import { EmbeddingFunction } from "./embedding_function";
import { EmbeddingFunctionConfig, getRegistry } from "./registry";

export {
  FieldOptions,
  EmbeddingFunction,
  TextEmbeddingFunction,
  FunctionOptions,
  EmbeddingFunctionConstructor,
} from "./embedding_function";

export * from "./registry";
export * from "./utils";

/**
 * Create a schema with embedding functions.
 *
 * @param fields
 * @returns Schema
 * @example
 * ```ts
 * class MyEmbeddingFunction extends EmbeddingFunction {
 * // ...
 * }
 * const func = new MyEmbeddingFunction();
 * const schema = LanceSchema({
 *   id: new Int32(),
 *   text: func.sourceField(new Utf8()),
 *   vector: func.vectorField(),
 *   // optional: specify the datatype and/or dimensions
 *   vector2: func.vectorField({ datatype: new Float32(), dims: 3}),
 * });
 *
 * const table = await db.createTable("my_table", data, { schema });
 * ```
 */
export function LanceSchema(
  fields: Record<string, [object, Map<string, EmbeddingFunction>] | object>,
): Schema {
  const arrowFields: Field[] = [];

  const embeddingFunctions = new Map<
    EmbeddingFunction,
    Partial<EmbeddingFunctionConfig>
  >();
  Object.entries(fields).forEach(([key, value]) => {
    if (Array.isArray(value)) {
      const [dtype, metadata] = value as [
        object,
        Map<string, EmbeddingFunction>,
      ];
      arrowFields.push(new Field(key, sanitizeType(dtype), true));
      parseEmbeddingFunctions(embeddingFunctions, key, metadata);
    } else {
      arrowFields.push(new Field(key, sanitizeType(value), true));
    }
  });
  const registry = getRegistry();
  const metadata = registry.getTableMetadata(
    Array.from(embeddingFunctions.values()) as EmbeddingFunctionConfig[],
  );
  const schema = new Schema(arrowFields, metadata);
  return schema;
}

function parseEmbeddingFunctions(
  embeddingFunctions: Map<EmbeddingFunction, Partial<EmbeddingFunctionConfig>>,
  key: string,
  metadata: Map<string, EmbeddingFunction>,
): void {
  if (metadata.has("source_column_for")) {
    const embedFunction = metadata.get("source_column_for")!;
    const current = embeddingFunctions.get(embedFunction);
    if (current !== undefined) {
      embeddingFunctions.set(embedFunction, {
        ...current,
        sourceColumn: key,
      });
    } else {
      embeddingFunctions.set(embedFunction, {
        sourceColumn: key,
        function: embedFunction,
      });
    }
  } else if (metadata.has("vector_column_for")) {
    const embedFunction = metadata.get("vector_column_for")!;

    const current = embeddingFunctions.get(embedFunction);
    if (current !== undefined) {
      embeddingFunctions.set(embedFunction, {
        ...current,
        vectorColumn: key,
      });
    } else {
      embeddingFunctions.set(embedFunction, {
        vectorColumn: key,
        function: embedFunction,
      });
    }
  }
}
