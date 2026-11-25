// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::{future::poll_fn, pin::pin, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    arrow::datatypes::SchemaRef,
    catalog::{Session, TableProvider},
    logical_expr::{Expr, TableProviderFilterPushDown, TableType},
    physical_plan::ExecutionPlan,
};
use datafusion_common::Result as DataFusionResult;
use datafusion_ffi::{
    proto::logical_extension_codec::FFI_LogicalExtensionCodec, table_provider::FFI_TableProvider,
};
use lancedb::{Table, table::datafusion::BaseTableAdapter};
use pyo3::{
    Bound, PyAny, PyResult, Python,
    exceptions::{PyImportError, PyNotImplementedError},
    types::{PyAnyMethods, PyCapsule, PyCapsuleMethods},
};
use tokio::runtime::Handle;

use crate::{error::PythonErrorExt, runtime::block_on_interruptible};

/// Runs scan planning with LanceDB's Tokio handle and exposes reads to DataFusion.
#[derive(Debug)]
struct DataFusionTable {
    adapter: BaseTableAdapter,
    runtime: Handle,
}

#[async_trait]
impl TableProvider for DataFusionTable {
    fn schema(&self) -> SchemaRef {
        self.adapter.schema()
    }

    fn table_type(&self) -> TableType {
        self.adapter.table_type()
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        self.adapter.supports_filters_pushdown(filters)
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let mut scan = pin!(self.adapter.scan(state, projection, filters, limit));
        // DataFusion's extension polls this future on its own threads. The FFI
        // handles the runtime for execution, but scan planning needs it too.
        poll_fn(|cx| {
            let _runtime = self.runtime.enter();
            scan.as_mut().poll(cx)
        })
        .await
    }
}

pub(super) fn table_provider<'py>(
    py: Python<'py>,
    table: &Table,
    session: Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyCapsule>> {
    if table.as_native().is_none() {
        return Err(PyNotImplementedError::new_err(
            "DataFusion table registration is only supported for LanceDB Embedded tables",
        ));
    }

    // Check the Python consumer's major version before reading its FFI struct.
    let major = datafusion_ffi::version();
    let version: String = py.import("datafusion")?.getattr("__version__")?.extract()?;
    if version.split('.').next().and_then(|v| v.parse().ok()) != Some(major) {
        return Err(PyImportError::new_err(format!(
            "LanceDB table registration requires DataFusion {major}.x, found {version}"
        )));
    }

    // SessionContext supplies a session; Schema registration supplies the codec
    // capsule directly. Both are part of DataFusion's table provider protocol.
    let capsule = if session.hasattr("__datafusion_logical_extension_codec__")? {
        session.call_method0("__datafusion_logical_extension_codec__")?
    } else {
        session
    };
    let capsule = capsule.cast::<PyCapsule>()?;
    let codec_ptr = capsule
        .pointer_checked(Some(c"datafusion_logical_extension_codec"))?
        .cast::<FFI_LogicalExtensionCodec>();
    // SAFETY: The named capsule comes from the DataFusion table provider
    // protocol above, and the Python consumer uses the same FFI major version.
    let codec = unsafe { codec_ptr.as_ref() };
    let codec_major = unsafe { (codec.version)() };
    if codec_major != major {
        return Err(PyImportError::new_err(format!(
            "LanceDB requires DataFusion codec ABI {major}, found {codec_major}"
        )));
    }
    // Retain the codec after the source capsule goes out of scope.
    let codec = codec.clone();

    let table = Arc::clone(table.base_table());
    let (adapter, runtime) = py.detach(|| {
        block_on_interruptible(async move {
            let adapter = BaseTableAdapter::try_new(table).await.infer_error()?;
            Ok((adapter, Handle::current()))
        })
    })?;
    let provider = FFI_TableProvider::new_with_ffi_codec(
        Arc::new(DataFusionTable {
            adapter,
            runtime: runtime.clone(),
        }),
        true,
        Some(runtime),
        codec,
    );
    PyCapsule::new_with_value(py, provider, c"datafusion_table_provider")
}
