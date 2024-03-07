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

use connection::{connect, Connection};
use env_logger::Env;
use index::{Index, IndexConfig};
use pyo3::{pymodule, types::PyModule, wrap_pyfunction, PyResult, Python};
use table::Table;

pub mod connection;
pub mod error;
pub mod index;
pub mod table;
pub mod util;

#[pymodule]
pub fn _lancedb(_py: Python, m: &PyModule) -> PyResult<()> {
    let env = Env::new()
        .filter_or("LANCEDB_LOG", "warn")
        .write_style("LANCEDB_LOG_STYLE");
    env_logger::init_from_env(env);
    m.add_class::<Connection>()?;
    m.add_class::<Table>()?;
    m.add_class::<Index>()?;
    m.add_class::<IndexConfig>()?;
    m.add_function(wrap_pyfunction!(connect, m)?)?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
