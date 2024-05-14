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

export {
  WriteOptions,
  WriteMode,
  AddColumnsSql,
  ColumnAlteration,
  ConnectionOptions,
} from "./native.js";
export {
  makeArrowTable,
  MakeArrowTableOptions,
  Data,
  VectorColumnOptions,
} from "./arrow";
export {
  connect,
  Connection,
  CreateTableOptions,
  TableNamesOptions,
} from "./connection";
export {
  ExecutableQuery,
  Query,
  QueryBase,
  VectorQuery,
  RecordBatchIterator,
} from "./query";
export { Index, IndexOptions, IvfPqOptions } from "./indices";
export { Table, AddDataOptions, IndexConfig, UpdateOptions } from "./table";
export * as embedding from "./embedding";
