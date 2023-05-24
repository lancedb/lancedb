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

function getPlatformLibrary() {
    if (process.platform === "darwin" && process.arch == "arm64") {
        return require('./aarch64-apple-darwin.node');
    } else if (process.platform === "darwin" && process.arch == "x64") {
        return require('./x86_64-apple-darwin.node');
    } else if (process.platform === "linux" && process.arch == "x64") {
        return require('./x86_64-unknown-linux-gnu.node');
    } else {
        throw new Error(`vectordb: unsupported platform ${process.platform}_${process.arch}. Please file a bug report at https://github.com/lancedb/lancedb/issues`)
    }
}

try {
    nativeLib = require('./index.node')
} catch (e) {
    if (e.code === "MODULE_NOT_FOUND") {
        nativeLib = getPlatformLibrary();
    } else {
        throw new Error('vectordb: failed to load native library. Please file a bug report at https://github.com/lancedb/lancedb/issues');
    }
}

module.exports = nativeLib

