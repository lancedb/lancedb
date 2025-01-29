// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import {
  type EmbeddingFunction,
  type EmbeddingFunctionConstructor,
} from "./embedding_function";
import "reflect-metadata";

type CreateReturnType<T> = T extends { init: () => Promise<void> }
  ? Promise<T>
  : T;

interface EmbeddingFunctionCreate<T extends EmbeddingFunction> {
  create(options?: T["TOptions"]): CreateReturnType<T>;
}

/**
 * This is a singleton class used to register embedding functions
 * and fetch them by name. It also handles serializing and deserializing.
 * You can implement your own embedding function by subclassing EmbeddingFunction
 * or TextEmbeddingFunction and registering it with the registry
 */
export class EmbeddingFunctionRegistry {
  #functions = new Map<string, EmbeddingFunctionConstructor>();

  /**
   * Get the number of registered functions
   */
  length() {
    return this.#functions.size;
  }

  /**
   * Register an embedding function
   * @param name The name of the function
   * @param func The function to register
   * @throws Error if the function is already registered
   */
  register<
    T extends EmbeddingFunctionConstructor = EmbeddingFunctionConstructor,
  >(
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
          `Embedding function with alias "${alias}" already exists`,
        );
      }
      self.#functions.set(alias, ctor);
      Reflect.defineMetadata("lancedb::embedding::name", alias, ctor);
      return ctor;
    };
  }

  get<T extends EmbeddingFunction<unknown>>(
    name: string,
  ): EmbeddingFunctionCreate<T> | undefined;
  /**
   * Fetch an embedding function by name
   * @param name The name of the function
   */
  get(name: string) {
    const factory = this.#functions.get(name);
    if (!factory) {
      // biome-ignore lint/suspicious/noExplicitAny: <explanation>
      return undefined as any;
    }
    // biome-ignore lint/suspicious/noExplicitAny: <explanation>
    let create: any;
    if (factory.prototype.init) {
      // biome-ignore lint/suspicious/noExplicitAny: <explanation>
      create = async function (options?: any) {
        const instance = new factory(options);
        await instance.init!();
        return instance;
      };
    } else {
      // biome-ignore lint/suspicious/noExplicitAny: <explanation>
      create = function (options?: any) {
        const instance = new factory(options);
        return instance;
      };
    }

    return {
      create,
    };
  }

  /**
   * reset the registry to the initial state
   */
  reset(this: EmbeddingFunctionRegistry) {
    this.#functions.clear();
  }

  /**
   * @ignore
   */
  async parseFunctions(
    this: EmbeddingFunctionRegistry,
    metadata: Map<string, string>,
  ): Promise<Map<string, EmbeddingFunctionConfig>> {
    if (!metadata.has("embedding_functions")) {
      return new Map();
    } else {
      type FunctionConfig = {
        name: string;
        sourceColumn: string;
        vectorColumn: string;
        model: EmbeddingFunction["TOptions"];
      };

      const functions = <FunctionConfig[]>(
        JSON.parse(metadata.get("embedding_functions")!)
      );

      const items: [string, EmbeddingFunctionConfig][] = await Promise.all(
        functions.map(async (f) => {
          const fn = this.get(f.name);
          if (!fn) {
            throw new Error(`Function "${f.name}" not found in registry`);
          }
          const func = await this.get(f.name)!.create(f.model);
          return [
            f.name,
            {
              sourceColumn: f.sourceColumn,
              vectorColumn: f.vectorColumn,
              function: func,
            },
          ];
        }),
      );

      return new Map(items);
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
    metadata["vectorColumn"] = conf.vectorColumn ?? "vector";
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

export function register(name?: string) {
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
