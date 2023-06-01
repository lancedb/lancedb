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

const { currentTarget } = require('@neon-rs/load');

let nativeLib;

try {
    nativeLib = require(`@lancedb/vectordb-${currentTarget()}`);
} catch (e) {
    try {
        // Might be developing locally, so try that. But don't expose that error
        // to the user.
        nativeLib = require("./index.node");
    } catch {
        throw new Error(`vectordb: failed to load native library.
  You may need to run \`npm install @lancedb/vectordb-${currentTarget()}\`.
      
  If that does not work, please file a bug report at https://github.com/lancedb/lancedb/issues
      
  Source error: ${e}`);
    }   
}

// Dynamic require for runtime.
module.exports = nativeLib;
