import { DataType, Field, Schema } from "apache-arrow";
import { EmbeddingFunction } from "./embedding_function";
import { EmbeddingFunctionConfig, getRegistry } from "./registry";

export { EmbeddingFunction } from "./embedding_function";
export * from "./openai";

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
  fields: Record<string, [DataType, Map<string, EmbeddingFunction>] | DataType>,
): Schema {
  const arrowFields: Field[] = [];

  const embeddingFunctions = new Map<
    EmbeddingFunction,
    Partial<EmbeddingFunctionConfig>
  >();
  Object.entries(fields).forEach(([key, value]) => {
    if (value instanceof DataType) {
      arrowFields.push(new Field(key, value));
    } else {
      const [dtype, metadata] = value;
      arrowFields.push(new Field(key, dtype));
      parseEmbeddingFunctions(embeddingFunctions, key, metadata);
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
