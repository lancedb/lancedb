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

import { Connection } from "./connection";
import { Connection as NativeConnection, ConnectionOptions } from "./native.js";

export {
  ConnectionOptions,
  WriteOptions,
  Query,
  MetricType,
} from "./native.js";
export { Connection } from "./connection";
export { Table } from "./table";
export { Data } from "./arrow";
export { IvfPQOptions, IndexBuilder } from "./indexer";

/**
 * Connect to a LanceDB instance at the given URI.
 *
 * Accpeted formats:
 *
 * - `/path/to/database` - local database
 * - `s3://bucket/path/to/database` or `gs://bucket/path/to/database` - database on cloud storage
 * - `db://host:port` - remote database (LanceDB cloud)
 *
 * @param uri The uri of the database. If the database uri starts with `db://` then it connects to a remote database.
 *
 * @see {@link ConnectionOptions} for more details on the URI format.
 */
export async function connect(uri: string): Promise<Connection>;
export async function connect(
  opts: Partial<ConnectionOptions>
): Promise<Connection>;
export async function connect(
  args: string | Partial<ConnectionOptions>
): Promise<Connection> {
  let opts: ConnectionOptions;
  if (typeof args === "string") {
    opts = { uri: args };
  } else {
    opts = Object.assign(
      {
        uri: "",
        apiKey: undefined,
        hostOverride: undefined,
      },
      args
    );
  }
  const nativeConn = await NativeConnection.new(opts);
  return new Connection(nativeConn);
}
