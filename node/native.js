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

let nativeLib;

if (process.platform === "darwin" && process.arch === "arm64") {
    nativeLib = require('./darwin_arm64.node')
} else if (process.platform === "linux" && process.arch === "x64") {
    nativeLib = require('./linux-x64.node')
} else {
    throw new Error(`vectordb: unsupported platform ${process.platform}_${process.arch}. Please file a bug report at https://github.com/lancedb/lancedb/issues`)
}

module.exports = nativeLib

