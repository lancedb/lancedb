import { Schema } from "apache-arrow";
import { Data } from "../arrow";
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

    // super();
    // super();
    const parsed = new URL(url);
    if (parsed.protocol !== "db:") {
      throw new Error(
        `invalid protocol: ${parsed.protocol}, only accepts db://`,
      );
    }
    apiKey = apiKey ?? process.env.LANCEDB_API_KEY;
    region = region ?? process.env.LANCEDB_REGION ?? "us-east-1";

    if (apiKey === undefined) {
      throw new Error("apiKey is required");
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
    throw new Error("Method not implemented.");
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
    return new RemoteTable(this.#client, name);
  }

  async createTable(
    tableName: string,
    data: Data,
    options?: Partial<CreateTableOptions> | undefined,
  ): Promise<Table> {
    if (options?.mode) {
      console.warn(`mode is not supported in remote connection, ignoring.`);
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
    return new RemoteTable(this.#client, tableName);
  }

  createEmptyTable(
    _name: string,
    _schema: Schema,
    _options?: Partial<CreateTableOptions> | undefined,
  ): Promise<Table> {
    throw new Error("createEmptyTable() is not supported in remote connection");
  }

  async dropTable(name: string): Promise<void> {
    await this.#client.post(`/v1/table/${encodeURIComponent(name)}/drop/`);

    this.#tableCache.delete(name);
  }
}
