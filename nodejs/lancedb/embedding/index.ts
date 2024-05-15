import { DataType, Field, Schema } from "apache-arrow";

export { EmbeddingFunction } from "./embedding_function";
export * from "./openai";

export function LanceSchema(
  options: Record<string, [DataType, Map<string, string>] | DataType>,
): Schema {
  const fields: Field[] = [];
  const metadata = new Map<string, string>();
  Object.entries(options).forEach(([key, value]) => {
    if (value instanceof DataType) {
      const field = new Field(key, value);
      fields.push(field);
    } else {
      const [dtype, metadataInner] = value;

      if (metadataInner.has("source_column")) {
        metadata.set("source_column", key);
      } else if (metadataInner.has("vector_column")) {
        metadata.set("vector_column", key);
      }

      if (metadataInner.has("model")) {
        metadata.set("model", metadataInner.get("model")!);
      }

      console.log("metadata", metadata);
      const field = new Field(key, dtype);

      fields.push(field);
    }
  });

  return new Schema(fields, metadata);
}
