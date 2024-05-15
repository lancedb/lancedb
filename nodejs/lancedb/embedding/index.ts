import { DataType, Field, Schema } from "apache-arrow";
import { EmbeddingFunctionConfig, getRegistry } from "./registry";
import { EmbeddingFunction } from "./embedding_function";

export { EmbeddingFunction } from "./embedding_function";
export * from "./openai";

export function LanceSchema(
  options: Record<
    string,
    [DataType, Map<string, EmbeddingFunction>] | DataType
  >,
): Schema {
  const fields: Field[] = [];

  const embeddingFunctions = new Map<
    EmbeddingFunction,
    Partial<EmbeddingFunctionConfig>
  >();
  Object.entries(options).forEach(([key, value]) => {
    if (value instanceof DataType) {
      fields.push(new Field(key, value));
    } else {
      const [dtype, metadata] = value;
      fields.push(new Field(key, dtype));
      parseEmbeddingFunctions(embeddingFunctions, key, metadata);
    }
  });
  const registry = getRegistry();
  const metadata = registry.getTableMetadata(
    Array.from(embeddingFunctions.values()) as EmbeddingFunctionConfig[],
  );
  const schema = new Schema(fields, metadata);
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
