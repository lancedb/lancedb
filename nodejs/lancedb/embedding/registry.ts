// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import {
  type EmbeddingFunction,
  type EmbeddingFunctionConstructor,
  type FunctionOptions,
  UntrustedEmbeddingFunction,
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

/** Options for {@link EmbeddingFunctionRegistry.register}. */
export interface RegisterOptions {
  /**
   * Whether table metadata may configure this function without approval.
   *
   * Only set this if the class's static `loadedModels` and `endpoints` methods
   * report every model and URL its options can make it load or contact. Those
   * still need approval. Otherwise, a function restored from table metadata
   * only computes embeddings once its options are approved, see
   * {@link EmbeddingFunctionRegistry.allowFunction}.
   */
  safeToRestore?: boolean;
}

/** Options for {@link EmbeddingFunctionRegistry.setVar}. */
export interface SetVarOptions {
  /**
   * Whether the value is a secret, such as an API key. Defaults to true.
   *
   * Secret variables can only be used in an embedding function's sensitive
   * keys, which send them only as credentials. Anyone who can write a table
   * chooses the variable names in its embedding options, so a secret in any
   * other option, such as a model name, could be shown to them in an error
   * message. Pass `secret: false` for settings such as a model name.
   */
  secret?: boolean;
}

/**
 * This is a singleton class used to register embedding functions
 * and fetch them by name. It also handles serializing and deserializing.
 * You can implement your own embedding function by subclassing EmbeddingFunction
 * or TextEmbeddingFunction and registering it with the registry
 */
export class EmbeddingFunctionRegistry {
  #functions = new Map<string, EmbeddingFunctionConstructor>();
  // Kept apart from the classes so subclasses don't inherit it: a subclass can
  // add options that load or contact something new.
  #safeToRestore = new Set<string>();
  #variables = new Map<string, string>();
  #secretVariables = new Set<string>();
  #allowedModels = new Set<string>();
  #allowedModelsAnyRevision = new Set<string>();
  #allowedEndpoints = new Set<string>();
  #allowedFunctions = new Set<string>();
  #trustedConfigs = new Set<string>();
  #restored = new WeakSet<EmbeddingFunction>();
  #resolvingFromMetadata = false;

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
    options: RegisterOptions = {},
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
      if (options.safeToRestore) {
        self.#safeToRestore.add(alias);
      }
      Reflect.defineMetadata("lancedb::embedding::name", alias, ctor);
      return ctor;
    };
  }

  /** @ignore */
  setBuiltIn<
    T extends EmbeddingFunctionConstructor = EmbeddingFunctionConstructor,
  >(name: string, ctor: T): T {
    this.#functions.set(name, ctor);
    this.#safeToRestore.add(name);
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
    this.#safeToRestore.clear();
    getBuiltInFunctions(this).clear();
  }

  /**
   * Trust a model when an embedding function that loads it is restored from
   * table metadata.
   *
   * Anyone who can write a table controls its embedding options, so table
   * metadata cannot pick a model other than the function's default by itself.
   * Embedding functions whose metadata this process wrote, for example by
   * creating a table with them, are allowed to load their models
   * automatically.
   *
   * @param model The model name, such as `"Xenova/bge-small-en-v1.5"`.
   * @param revision The revision the table's options must pin. If omitted,
   * any revision is trusted.
   */
  allowModel(model: string, revision?: string): void {
    if (revision === undefined) {
      this.#allowedModelsAnyRevision.add(model);
    } else {
      this.#allowedModels.add(modelKey(model, revision));
    }
  }

  /**
   * Trust a custom endpoint when an embedding function that sends data to it
   * is restored from table metadata. Trailing slashes are ignored.
   *
   * Embedding functions whose metadata this process wrote are allowed to use
   * their endpoints automatically.
   */
  allowEndpoint(url: string): void {
    this.#allowedEndpoints.add(normalizeUrl(url));
  }

  /**
   * Trust any options from table metadata for an embedding function that
   * isn't registered with `safeToRestore: true`, such as your own embedding
   * functions.
   *
   * LanceDB can't tell what such a function's options make it load or
   * contact, so when it is restored from table metadata it only computes
   * embeddings for options this process wrote, unless you call this. Models
   * and endpoints the function reports still need approval with
   * {@link allowModel} and {@link allowEndpoint}.
   *
   * @param name The name the function was registered under.
   */
  allowFunction(name: string): void {
    this.#allowedFunctions.add(name);
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
        const func = await this.#restore(f.name, f.model ?? {});
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

  /**
   * Create an embedding function from table metadata. Anyone who can write
   * the table controls `options`, so options that load a model or send data
   * elsewhere must already be trusted by this process.
   */
  async #restore(
    name: string,
    options: FunctionOptions,
  ): Promise<EmbeddingFunction> {
    const ctor = this.#functions.get(name);
    if (!ctor) {
      throw new Error(`Function "${name}" not found in registry`);
    }
    // Sensitive values such as API keys stay unresolved until the options are
    // approved, so neither the checks nor their messages can contain them.
    const publicOptions = this.#resolvePublicOptions(
      options,
      sensitiveKeysOf(ctor),
    );

    const reasons: string[] = [];
    if (
      !this.#safeToRestore.has(name) &&
      !this.#allowedFunctions.has(name) &&
      !this.#trustedConfigs.has(configKey(name, options))
    ) {
      reasons.push(
        "is not registered with safeToRestore: true, so its options need " +
          "approval. If you trust any options for it from table metadata, " +
          `call getRegistry().allowFunction(${JSON.stringify(name)})`,
      );
    }
    for (const [model, revision] of ctor.loadedModels?.(publicOptions) ?? []) {
      if (!this.#modelAllowed(model, revision)) {
        const args = [model, revision]
          .filter((arg) => arg !== undefined)
          .map((arg) => JSON.stringify(arg))
          .join(", ");
        reasons.push(
          `loads model ${JSON.stringify(model)}` +
            (revision === undefined
              ? ""
              : ` at revision ${JSON.stringify(revision)}`) +
            `. If you trust it, call getRegistry().allowModel(${args})`,
        );
      }
    }
    for (const url of ctor.endpoints?.(publicOptions) ?? []) {
      if (!this.#allowedEndpoints.has(normalizeUrl(url))) {
        reasons.push(
          `sends data to endpoint ${JSON.stringify(url)}. If you trust it, ` +
            `call getRegistry().allowEndpoint(${JSON.stringify(url)})`,
        );
      }
    }
    if (reasons.length > 0) {
      return new UntrustedEmbeddingFunction(name, options, reasons);
    }

    let func: EmbeddingFunction;
    this.#resolvingFromMetadata = true;
    try {
      func = new ctor(options);
    } finally {
      this.#resolvingFromMetadata = false;
    }
    await func.init?.();
    this.#restored.add(func);
    return func;
  }

  #modelAllowed(model: string, revision: string | undefined): boolean {
    return (
      this.#allowedModelsAnyRevision.has(model) ||
      this.#allowedModels.has(modelKey(model, revision))
    );
  }

  /** Non-sensitive options, with the variables metadata may use resolved. */
  #resolvePublicOptions(
    options: FunctionOptions,
    sensitiveKeys: string[],
  ): FunctionOptions {
    const resolved: FunctionOptions = {};
    for (const [key, value] of Object.entries(options)) {
      if (sensitiveKeys.includes(key)) {
        continue;
      }
      const reference = parseVariableReference(value);
      if (reference === undefined) {
        resolved[key] = value;
      } else if (
        this.#variables.has(reference.name) &&
        !this.#secretVariables.has(reference.name)
      ) {
        resolved[key] = this.#variables.get(reference.name);
      } else if (reference.defaultValue !== undefined) {
        resolved[key] = reference.defaultValue;
      } else {
        // The same error as for an unset variable, so a secret variable's
        // existence can't be probed.
        throw new Error(`Variable "${reference.name}" not found`);
      }
    }
    return resolved;
  }

  /**
   * The options this process wrote into table metadata are its own, so trust
   * them when the table is opened again.
   */
  #trustOwnFunction(name: string, func: EmbeddingFunction) {
    if (
      func instanceof UntrustedEmbeddingFunction ||
      this.#restored.has(func)
    ) {
      return;
    }
    const ctor = func.constructor as EmbeddingFunctionConstructor;
    const options = func.toJSON();
    if (!this.#safeToRestore.has(name)) {
      this.#trustedConfigs.add(configKey(name, options));
    }
    const publicOptions = this.#resolvePublicOptions(
      options,
      sensitiveKeysOf(ctor),
    );
    for (const [model, revision] of ctor.loadedModels?.(publicOptions) ?? []) {
      this.#allowedModels.add(modelKey(model, revision));
    }
    for (const url of ctor.endpoints?.(publicOptions) ?? []) {
      this.allowEndpoint(url);
    }
  }

  /**
   * @ignore
   * Resolve `$var:` references in an embedding function's options.
   */
  resolveVariables(
    options: FunctionOptions,
    sensitiveKeys: string[],
  ): FunctionOptions {
    const resolved = { ...options };
    for (const [key, value] of Object.entries(options)) {
      const sensitive = sensitiveKeys.includes(key);
      const reference = parseVariableReference(value);
      if (sensitive && typeof value === "string" && reference === undefined) {
        throw new Error(
          `The key "${key}" is sensitive and cannot be set directly. Please use the $var: syntax to set it.`,
        );
      }
      if (reference === undefined) {
        continue;
      }
      const { name, defaultValue } = reference;
      let found = this.#variables.has(name);
      if (found && !sensitive && this.#secretVariables.has(name)) {
        if (!this.#resolvingFromMetadata) {
          throw new Error(
            `Variable "${name}" is secret, so it can only be used in ` +
              `sensitive keys such as API keys, not "${key}". If it isn't a ` +
              "secret, set it with setVar(name, value, { secret: false }).",
          );
        }
        // From table metadata, behave as if it were unset, so the result
        // doesn't reveal whether the variable exists.
        found = false;
      }
      if (found) {
        resolved[key] = this.#variables.get(name);
      } else if (defaultValue !== undefined) {
        resolved[key] = defaultValue;
      } else {
        throw new Error(`Variable "${name}" not found`);
      }
    }
    return resolved;
  }

  // biome-ignore lint/suspicious/noExplicitAny: <explanation>
  functionToMetadata(conf: EmbeddingFunctionConfig): Record<string, any> {
    // biome-ignore lint/suspicious/noExplicitAny: <explanation>
    const metadata: Record<string, any> = {};
    const func = conf.function;
    const name =
      func instanceof UntrustedEmbeddingFunction
        ? func.functionName
        : (Reflect.getMetadata("lancedb::embedding::name", func.constructor) ??
          func.constructor.name);
    this.#trustOwnFunction(name, func);
    metadata["sourceColumn"] = conf.sourceColumn;
    metadata["vectorColumn"] = conf.vectorColumn ?? "vector";
    metadata["name"] = name;
    metadata["model"] = func.toJSON();
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
   * Variables are secret unless you pass `{ secret: false }`, and secret
   * variables can only be used in sensitive keys such as API keys. See
   * {@link SetVarOptions.secret}.
   *
   * @param name
   * @param value
   * @param options
   */
  setVar(name: string, value: string, options: SetVarOptions = {}): void {
    if (name.includes(":")) {
      throw new Error("Variable names cannot contain colons");
    }
    this.#variables.set(name, value);
    if (options.secret ?? true) {
      this.#secretVariables.add(name);
    } else {
      this.#secretVariables.delete(name);
    }
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

function parseVariableReference(
  value: unknown,
): { name: string; defaultValue?: string } | undefined {
  if (typeof value !== "string" || !value.startsWith("$var:")) {
    return undefined;
  }
  const rest = value.slice(5);
  const colon = rest.indexOf(":");
  return colon === -1
    ? { name: rest }
    : { name: rest.slice(0, colon), defaultValue: rest.slice(colon + 1) };
}

function sensitiveKeysOf(ctor: EmbeddingFunctionConstructor): string[] {
  // getSensitiveKeys is an instance method, but the options must be checked
  // before the constructor runs, since it may load a model.
  const prototype = ctor.prototype as { getSensitiveKeys?: () => string[] };
  return prototype.getSensitiveKeys?.call(prototype) ?? [];
}

function modelKey(model: string, revision: string | undefined): string {
  return JSON.stringify([model, revision ?? null]);
}

function normalizeUrl(url: string): string {
  return url.replace(/\/+$/, "");
}

function configKey(name: string, options: FunctionOptions): string {
  return JSON.stringify([name, sortKeys(options)]);
}

function sortKeys(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map(sortKeys);
  }
  if (value !== null && typeof value === "object") {
    return Object.fromEntries(
      Object.keys(value)
        .sort()
        .map((key) => [key, sortKeys((value as Record<string, unknown>)[key])]),
    );
  }
  return value;
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

export function register(name?: string, options?: RegisterOptions) {
  return _REGISTRY.register(name, options);
}

/** @ignore */
export function registerBuiltIn<
  T extends EmbeddingFunctionConstructor = EmbeddingFunctionConstructor,
>(name: string, ctor: T): T {
  const builtInFunctions = getBuiltInFunctions(_REGISTRY);
  if (builtInFunctions.has(name)) {
    return _REGISTRY.setBuiltIn(name, ctor);
  }
  // Built-in providers report everything their options can load or contact.
  _REGISTRY.register(name, { safeToRestore: true })(ctor);
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
