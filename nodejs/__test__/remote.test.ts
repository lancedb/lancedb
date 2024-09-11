// Copyright 2024 Lance Developers.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import axios, { AxiosInstance } from "axios";
import * as lancedb from "../lancedb";

// These test are unit tests against mocked endpoint responses. These responses
// are based on the OpenAPI spec for LanceDB Cloud. See the API spec at:
// https://lancedb.github.io/lancedb/cloud/rest/
jest.mock("axios");
const mockedAxios = axios as jest.Mocked<typeof axios>;
mockedAxios.create = jest.fn(() => mockedAxios);

describe("RemoteConnection constructor", () => {
  it("should require an apiKey", () => {
    expect(async () => {
      await lancedb.connect("db://test", { region: "us-west-2" });
    }).rejects.toThrow("apiKey is required when connecting to LanceDB Cloud");
  });
});

describe("RemoteConnection#tableNames", () => {
  it("should be able to list tables", async () => {
    const db = await lancedb.connect("db://test", {
      apiKey: "test",
      region: "us-west-2",
    });

    mockedAxios.get.mockResolvedValue({
      status: 200,
      data: {
        tables: ["table1", "table2"],
      },
    });
    const tables = await db.tableNames();
    expect(tables).toEqual(["table1", "table2"]);
  });

  it("should be able to list tables with pagination", async () => {
    const db = await lancedb.connect("db://test", {
      apiKey: "test",
      region: "us-west-2",
    });
    mockedAxios.get.mockResolvedValue({
      status: 200,
      data: {
        tables: ["table3", "table4"],
      },
    });
    const tables = await db.tableNames({ limit: 2, startAfter: "table2" });
    expect(tables).toEqual(["table3", "table4"]);
    expect(mockedAxios.get).toHaveBeenCalledWith(
      "https://test.us-west-2.api.lancedb.com/v1/table/",
      {
        params: {
          limit: 2,
          page_token: "table2",
        },
      },
    );
  });
});

describe("RemoteConnection#openTable", () => {
  it("should make a valid open table request", async () => {
    const _db = await lancedb.connect("db://test", {
      apiKey: "test",
      region: "us-west-2",
    });
  });

  it("should raise a TableNotFoundError if the table does not exist", async () => {});
});

describe("RemoteConnection#createTable", () => {
  it("should make a valid create table request", async () => {});

  it("should raise an error if mode other than 'create' is passed", async () => {});

  it("should raise an error if you try to pass embedding functions", async () => {});

  it("should raise a TableAlreadyExistsError if the table already exists", async () => {});
});

describe("RemoteConnection#createEmptyTable", () => {
  it("should make a valid create empty table request", async () => {});

  it("should raise an error if mode other than 'create' is passed", async () => {});

  it("should raise an error if you try to pass embedding functions", async () => {});
});

describe("RemoteConnection#dropTable", () => {
  it("should make a valid drop table request", async () => {});

  it("should raise a TableNotFoundError if the table does not exist", async () => {});
});

// describe("RemoteTable#schema", () => {
//     it("should make a valid schema request", async () => {
//     })

//     it("should raise a TableNotFoundError if the table does not exist", async () => {
//     })
// })

// describe("RemoteTable#add", () => {
//     it("should make a valid add request", async () => {
//     })
// })

// describe("RemoteTable#update", () => {
//     // TODO: this endpoint is not documented in the openapi spec yet.
//     it("should make a valid update with SQL request", async () => {
//     })

//     it("should make a valid update with values request", async () => {
//     })
// })

// describe("RemoteTable#countRows", () => {
//     it("should make a valid count rows request", async () => {
//     })
// })

// describe("RemoteTable#delete", () => {
//     it("should make a valid delete request", async () => {
//     })
// })

// describe("RemoteTable#createIndex", () => {
//     it("should make a valid create index request", async () => {
//         // TODO: should pass through the column, metric_type, index_type
//     })

//     it("should error if unsupported options are passed", async () => {
//     })
// })

// describe("RemoteTable#search", () => {
//     it("should make a valid query request", async () => {
//     })
// })

// describe("RemoteTable#indexStats", () => {
//     it("should make a valid vector search request", async () => {
//     })
// })
