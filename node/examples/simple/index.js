// Copyright 2023 Lance Developers.
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

'use strict'

async function example() {
    const lancedb = require('vectordb');
    const db = lancedb.connect('../../sample-lancedb');

    console.log(db.tableNames());

    const tbl = await db.openTable('my_table');
    const query = tbl.search([0.1, 0.3]);
    const results = await query.execute();
    console.log(results);
}

example();


