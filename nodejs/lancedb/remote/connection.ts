import { Schema } from "apache-arrow";
import {
  Data,
  SchemaLike,
  fromTableToStreamBuffer,
  makeEmptyTable,
} from "../arrow";
import {
  Connection,
  CreateTableOptions,
  OpenTableOptions,
  TableNamesOptions,
} from "../connection";
import { Table } from "../table";
import { TTLCache } from "../util";
import { RestfulLanceDBClient } from "./client";
import { RemoteTable } from "./table";

export interface RemoteConnectionOptions {
  apiKey?: string;
  region?: string;
  hostOverride?: string;
  timeout?: number;
}

export class RemoteConnection extends Connection {
  #dbName: string;
  #apiKey: string;
  #region: string;
  #client: RestfulLanceDBClient;
  #tableCache = new TTLCache(300_000);

  constructor(
    url: string,
    { apiKey, region, hostOverride, timeout }: RemoteConnectionOptions,
  ) {
    super();
    apiKey = apiKey ?? process.env.LANCEDB_API_KEY;
    region = region ?? process.env.LANCEDB_REGION;

    if (!apiKey) {
      throw new Error("apiKey is required when connecting to LanceDB Cloud");
    }

    if (!region) {
      throw new Error("region is required when connecting to LanceDB Cloud");
    }

    const parsed = new URL(url);
    if (parsed.protocol !== "db:") {
      throw new Error(
        `invalid protocol: ${parsed.protocol}, only accepts db://`,
      );
    }

    this.#dbName = parsed.hostname;
    this.#apiKey = apiKey;
    this.#region = region;
    this.#client = new RestfulLanceDBClient(
      this.#dbName,
      this.#apiKey,
      this.#region,
      hostOverride,
      timeout,
    );
  }

  isOpen(): boolean {
    return this.#client.isOpen();
  }
  close(): void {
    return this.#client.close();
  }

  display(): string {
    return `RemoteConnection(${this.#dbName})`;
  }

  async tableNames(options?: Partial<TableNamesOptions>): Promise<string[]> {
    const response = await this.#client.get("/v1/table/", {
      limit: options?.limit ?? 10,
      // biome-ignore lint/style/useNamingConvention: <explanation>
      page_token: options?.startAfter ?? "",
    });
    const body = await response.body();
    for (const table of body.tables) {
      this.#tableCache.set(table, true);
    }
    return body.tables;
  }

  async openTable(
    name: string,
    _options?: Partial<OpenTableOptions> | undefined,
  ): Promise<Table> {
    if (this.#tableCache.get(name) === undefined) {
      await this.#client.post(
        `/v1/table/${encodeURIComponent(name)}/describe/`,
      );
      this.#tableCache.set(name, true);
    }
    return new RemoteTable(this.#client, name, this.#dbName);
  }

  async createTable(
    nameOrOptions:
      | string
      | ({ name: string; data: Data } & Partial<CreateTableOptions>),
    data?: Data,
    options?: Partial<CreateTableOptions> | undefined,
  ): Promise<Table> {
    if (typeof nameOrOptions !== "string" && "name" in nameOrOptions) {
      const { name, data, ...options } = nameOrOptions;
      return this.createTable(name, data, options);
    }
    if (data === undefined) {
      throw new Error("data is required");
    }
    if (options?.mode) {
      console.warn(
        "option 'mode' is not supported in LanceDB Cloud",
        "LanceDB Cloud only supports the default 'create' mode.",
        "If the table already exists, an error will be thrown.",
      );
    }
    if (options?.embeddingFunction) {
      console.warn(
        "embedding_functions is not yet supported on LanceDB Cloud.",
        "Please vote https://github.com/lancedb/lancedb/issues/626 ",
        "for this feature.",
      );
    }

    const { buf } = await Table.parseTableData(
      data,
      options,
      true /** streaming */,
    );

    await this.#client.post(
      `/v1/table/${encodeURIComponent(nameOrOptions)}/create/`,
      buf,
      {
        config: {
          responseType: "arraybuffer",
        },
        headers: { "Content-Type": "application/vnd.apache.arrow.stream" },
      },
    );
    this.#tableCache.set(nameOrOptions, true);
    return new RemoteTable(this.#client, nameOrOptions, this.#dbName);
  }

  async createEmptyTable(
    name: string,
    schema: SchemaLike,
    options?: Partial<CreateTableOptions> | undefined,
  ): Promise<Table> {
    if (options?.mode) {
      console.warn(`mode is not supported on LanceDB Cloud`);
    }

    if (options?.embeddingFunction) {
      console.warn(
        "embeddingFunction is not yet supported on LanceDB Cloud.",
        "Please vote https://github.com/lancedb/lancedb/issues/626 ",
        "for this feature.",
      );
    }
    const emptyTable = makeEmptyTable(schema);
    const buf = await fromTableToStreamBuffer(emptyTable);

    await this.#client.post(
      `/v1/table/${encodeURIComponent(name)}/create/`,
      buf,
      {
        config: {
          responseType: "arraybuffer",
        },
        headers: { "Content-Type": "application/vnd.apache.arrow.stream" },
      },
    );

    this.#tableCache.set(name, true);
    return new RemoteTable(this.#client, name, this.#dbName);
  }

  async dropTable(name: string): Promise<void> {
    await this.#client.post(`/v1/table/${encodeURIComponent(name)}/drop/`);

    this.#tableCache.delete(name);
  }
}
