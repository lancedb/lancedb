import { Table as ArrowTable, Schema } from "apache-arrow";
import { Data, fromTableToStreamBuffer, makeEmptyTable } from "../arrow";
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
  region: string;
  hostOverride?: string;
  connectionTimeout?: number;
  readTimeout?: number;
}
export class RemoteConnection extends Connection {
  #dbName: string;
  #apiKey: string;
  #region?: string;
  #client!: RestfulLanceDBClient;
  #tableCache = new TTLCache(300_000);

  [Symbol.toStringTag] = "--RemoteConnection--";
  [Symbol.for("Jupyter.display")](): string {
    return this.display();
  }
  [Symbol.for("nodejs.util.inspect.custom")](): string {
    return this.display();
  }

  constructor(
    url: string,
    {
      apiKey,
      region,
      hostOverride,
      connectionTimeout,
      readTimeout,
    }: RemoteConnectionOptions,
  ) {
    super();
    apiKey = apiKey ?? process.env.LANCEDB_API_KEY;
    region = region ?? process.env.LANCEDB_REGION ?? "us-east-1";

    if (!apiKey) {
      throw new Error("apiKey is required when connecting to LanceDB Cloud");
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
      connectionTimeout,
      readTimeout,
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
    tableName: string,
    data: Data,
    options?: Partial<CreateTableOptions> | undefined,
  ): Promise<Table> {
    if (options?.mode) {
      console.warn(`mode is not supported in remote connection, ignoring.`);
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
      `/v1/table/${encodeURIComponent(tableName)}/create/`,
      buf,
      {
        config: {
          responseType: "arraybuffer",
        },
        headers: { "Content-Type": "application/vnd.apache.arrow.stream" },
      },
    );
    this.#tableCache.set(tableName, true);
    return new RemoteTable(this.#client, tableName, this.#dbName);
  }

  async createEmptyTable(
    name: string,
    schema: Schema,
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
