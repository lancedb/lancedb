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

import * as lancedb from "@lancedb/lancedb";

const db = await lancedb.connect("data/sample-lancedb");

const words = [
  "apple",
  "banana",
  "cherry",
  "date",
  "elderberry",
  "fig",
  "grape",
];

const data = Array.from({ length: 10_000 }, (_, i) => ({
  vector: Array(1536).fill(i),
  id: i,
  item: `item ${i}`,
  strId: `${i}`,
  doc: words[i % words.length],
}));

const tbl = await db.createTable("myVectors", data, { mode: "overwrite" });

await tbl.createIndex("doc", {
  config: lancedb.Index.fts(),
});

// --8<-- [start:full_text_search]
let result = await tbl
  .search("apple")
  .select(["id", "doc"])
  .limit(10)
  .toArray();
console.log(result);
// --8<-- [end:full_text_search]

console.log("SQL search: done");
