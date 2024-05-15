import { EmbeddingFunction } from "./embedding_function";
import "reflect-metadata";
export interface EmbeddingFunctionOptions {
  [key: string]: unknown;
}

export interface EmbeddingFunctionFactory<
  T extends EmbeddingFunction = EmbeddingFunction,
> {
  new (modelOptions?: EmbeddingFunctionOptions): T;
}

interface EmbeddingFunctionCreate<T extends EmbeddingFunction> {
  create(options?: EmbeddingFunctionOptions): T;
}

/**
 * This is a singleton class used to register embedding functions
 * and fetch them by name. It also handles serializing and deserializing.
 * You can implement your own embedding function by subclassing EmbeddingFunction
 * or TextEmbeddingFunction and registering it with the registry
 */
export class EmbeddingFunctionRegistry {
  #functions: Map<string, EmbeddingFunctionFactory> = new Map();

  static getInstance() {
    return _REGISTRY;
  }

  /**
   * Register an embedding function
   * @param name The name of the function
   * @param func The function to register
   */
  register<T extends EmbeddingFunctionFactory = EmbeddingFunctionFactory>(
    this: EmbeddingFunctionRegistry,
    alias?: string,
    // biome-ignore lint/suspicious/noExplicitAny: <explanation>
  ): (ctor: T) => any {
    const self = this;
    return function (ctor: T) {
      if (!alias) {
        alias = ctor.name;
      }
      if (self.#functions.has(alias)) {
        throw new Error(
          `Embedding function with alias ${alias} already exists`,
        );
      }
      self.#functions.set(alias, ctor);
      Reflect.defineMetadata("lancedb::embedding::name", alias, ctor);
      return ctor;
    };
  }

  /**
   * Fetch an embedding function by name
   * @param name The name of the function
   */
  get<T extends EmbeddingFunction<unknown> = EmbeddingFunction>(
    name: string,
  ): EmbeddingFunctionCreate<T> | undefined {
    const factory = this.#functions.get(name);
    if (!factory) {
      return undefined;
    }
    return {
      create: function (options: EmbeddingFunctionOptions) {
        return new factory(options) as unknown as T;
      },
    };
  }

  /**
   * reset the registry to the initial state
   */
  reset(this: EmbeddingFunctionRegistry) {
    this.#functions.clear();
  }

  parseFunctions(
    this: EmbeddingFunctionRegistry,
    metadata: Map<string, string>,
  ): Map<string, EmbeddingFunctionConfig> {
    if (!metadata.has("embedding_functions")) {
      return new Map();
    } else {
      type FunctionConfig = {
        name: string;
        sourceColumn: string;
        vectorColumn: string;
        model: EmbeddingFunctionOptions;
      };
      const functions = JSON.parse(
        metadata.get("embedding_functions")!,
      ) as FunctionConfig[];
      return new Map(
        functions.map((f) => {
          return [
            f.name,
            {
              sourceColumn: f.sourceColumn,
              vectorColumn: f.vectorColumn,
              function: this.get(f.name)!.create(f.model),
            },
          ];
        }),
      );
    }
  }
  // biome-ignore lint/suspicious/noExplicitAny: <explanation>
  functionToMetadata(conf: EmbeddingFunctionConfig): Record<string, any> {
    // biome-ignore lint/suspicious/noExplicitAny: <explanation>
    const metadata: Record<string, any> = {};
    const name = Reflect.getMetadata(
      "lancedb::embedding::name",
      conf.function.constructor,
    );
    metadata["sourceColumn"] = conf.sourceColumn;
    metadata["vectorColumn"] = conf.vectorColumn;
    metadata["name"] = name ?? conf.function.constructor.name;
    metadata["model"] = conf.function.toJSON();
    return metadata;
  }

  getTableMetadata(functions: EmbeddingFunctionConfig[]): Map<string, string> {
    const metadata = new Map<string, string>();
    const jsonData = functions.map((conf) => this.functionToMetadata(conf));
    metadata.set("embedding_functions", JSON.stringify(jsonData));

    return metadata;
  }
}

const _REGISTRY = new EmbeddingFunctionRegistry();

export function register(name: string) {
  return _REGISTRY.register(name);
}

/**
 * Utility function to get the global instance of the registry
 * @returns `EmbeddingFunctionRegistry` The global instance of the registry
 * @example
 * ```ts
 * const registry = getRegistry();
 * const openai = registry.get("openai").create();
 */
export function getRegistry(): EmbeddingFunctionRegistry {
  return _REGISTRY;
}

export interface EmbeddingFunctionConfig {
  sourceColumn: string;
  vectorColumn?: string;
  function: EmbeddingFunction;
}
