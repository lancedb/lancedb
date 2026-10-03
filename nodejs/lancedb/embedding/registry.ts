// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import {
  type EmbeddingFunction,
  type EmbeddingFunctionConstructor,
} from "./embedding_function";
import "reflect-metadata";

const builtInFunctionsKey = Symbol.for(
  "@lancedb/lancedb::embedding-built-in-functions::v1",
);

export type CreateReturnType<T> = T extends { init: () => Promise<void> }
  ? Promise<T>
  : T;

export interface EmbeddingFunctionCreate<T extends EmbeddingFunction> {
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
  #variables = new Map<string, string>();

  /**
   * Get the number of registered functions
   */
  length() {
    return this.#functions.size;
  }

  /**
   * Register an embedding function
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

  /** @ignore */
  setBuiltIn<
    T extends EmbeddingFunctionConstructor = EmbeddingFunctionConstructor,
  >(name: string, ctor: T): T {
    this.#functions.set(name, ctor);
    Reflect.defineMetadata("lancedb::embedding::name", name, ctor);
    return ctor;
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
      create = (options?: any) => new factory(options);
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
    getBuiltInFunctions(this).clear();
  }

  /**
   * @ignore
   */
  async parseFunctions(
    this: EmbeddingFunctionRegistry,
    metadata: Map<string, string>,
  ): Promise<Map<string, ResolvedEmbeddingFunctionConfig>> {
    if (!metadata.has("embedding_functions")) {
      return new Map();
    }
    const entries = parseEmbeddingMetadata(
      metadata.get("embedding_functions")!,
    );
    const items = await Promise.all(
      entries.map(async (f): Promise<ResolvedEmbeddingFunctionConfig> => {
        const fn = this.get(f.name);
        if (!fn) {
          throw new Error(`Function "${f.name}" not found in registry`);
        }
        const func = await fn.create(f.model);
        return {
          sourceColumn: f.sourceColumn,
          vectorColumn: f.vectorColumn,
          function: func,
        };
      }),
    );
    // Keyed by output column: one function may serve several columns.
    return new Map(items.map((config) => [config.vectorColumn, config]));
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

  /**
   * Set a variable. These can be accessed in the embedding function
   * configuration using the syntax `$var:variable_name`. If they are not
   * set, an error will be thrown letting you know which key is unset. If you
   * want to supply a default value, you can add an additional part in the
   * configuration like so: `$var:variable_name:default_value`. Default values
   * can be used for runtime configurations that are not sensitive, such as
   * whether to use a GPU for inference.
   *
   * The name must not contain colons. The default value can contain colons.
   *
   * @param name
   * @param value
   */
  setVar(name: string, value: string): void {
    if (name.includes(":")) {
      throw new Error("Variable names cannot contain colons");
    }
    this.#variables.set(name, value);
  }

  /**
   * Get a variable.
   * @param name
   * @returns
   * @see {@link setVar}
   */
  getVar(name: string): string | undefined {
    return this.#variables.get(name);
  }
}

function getBuiltInFunctions(registry: EmbeddingFunctionRegistry): Set<string> {
  const registryWithBuiltIns = registry as EmbeddingFunctionRegistry & {
    [key: symbol]: Set<string> | undefined;
  };
  let builtInFunctions = registryWithBuiltIns[builtInFunctionsKey];
  if (builtInFunctions === undefined) {
    builtInFunctions = new Set<string>();
    registryWithBuiltIns[builtInFunctionsKey] = builtInFunctions;
  }
  return builtInFunctions;
}

// Server bundlers can load the side-effect embedding entry points and the public
// embedding API from separate module graphs. Keep their registry shared.
const registryKey = Symbol.for(
  "@lancedb/lancedb::embedding-function-registry::v1",
);
const registryGlobal = globalThis as typeof globalThis & {
  [key: symbol]: EmbeddingFunctionRegistry | undefined;
};

function getGlobalRegistry(): EmbeddingFunctionRegistry {
  const existingRegistry = registryGlobal[registryKey];
  if (existingRegistry !== undefined) {
    return existingRegistry;
  }
  const registry = new EmbeddingFunctionRegistry();
  registryGlobal[registryKey] = registry;
  return registry;
}

const _REGISTRY = getGlobalRegistry();

export function register(name?: string) {
  return _REGISTRY.register(name);
}

/** @ignore */
export function registerBuiltIn<
  T extends EmbeddingFunctionConstructor = EmbeddingFunctionConstructor,
>(name: string, ctor: T): T {
  const builtInFunctions = getBuiltInFunctions(_REGISTRY);
  if (builtInFunctions.has(name)) {
    return _REGISTRY.setBuiltIn(name, ctor);
  }
  _REGISTRY.register(name)(ctor);
  builtInFunctions.add(name);
  return ctor;
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

/** An [EmbeddingFunctionConfig] read back from table metadata, where the
 * vector column is always recorded. */
export type ResolvedEmbeddingFunctionConfig = EmbeddingFunctionConfig & {
  vectorColumn: string;
};

/** One entry of the `embedding_functions` schema metadata, with the column
 * keys normalized across the bindings' spellings. */
export type EmbeddingMetadataEntry = {
  name: string;
  sourceColumn: string;
  vectorColumn: string;
  model: EmbeddingFunction["TOptions"];
};

/** The single parser for `embedding_functions` schema metadata: every reader
 * goes through here, so the wire contract cannot fork between them. */
export function parseEmbeddingMetadata(json: string): EmbeddingMetadataEntry[] {
  // The wire format, honestly: the Python bindings write snake_case keys.
  type Raw = {
    name: string;
    sourceColumn?: string;
    // biome-ignore lint/style/useNamingConvention: the Python wire spelling
    source_column?: string;
    vectorColumn?: string;
    // biome-ignore lint/style/useNamingConvention: the Python wire spelling
    vector_column?: string;
    model: EmbeddingFunction["TOptions"];
  };
  const entries = <Raw[]>JSON.parse(json);
  const seen = new Set<string>();
  return entries.map((f) => {
    const sourceColumn = f.sourceColumn ?? f.source_column;
    const vectorColumn = f.vectorColumn ?? f.vector_column;
    if (sourceColumn === undefined || vectorColumn === undefined) {
      throw new Error(
        `Embedding function "${f.name}" metadata names no source or vector column`,
      );
    }
    if (seen.has(vectorColumn)) {
      throw new Error(
        `Multiple embedding configs claim vector column "${vectorColumn}"`,
      );
    }
    seen.add(vectorColumn);
    return { name: f.name, sourceColumn, vectorColumn, model: f.model };
  });
}
