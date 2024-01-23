use std::sync::Arc;

use lance::{
    dataset::{ReadParams, WriteParams},
};
use lance::io::{ObjectStoreParams, WrappingObjectStore};

use crate::error::{Error, Result};

pub trait PatchStoreParam {
    fn patch_with_store_wrapper(
        self,
        wrapper: Arc<dyn WrappingObjectStore>,
    ) -> Result<Option<ObjectStoreParams>>;
}

impl PatchStoreParam for Option<ObjectStoreParams> {
    fn patch_with_store_wrapper(
        self,
        wrapper: Arc<dyn WrappingObjectStore>,
    ) -> Result<Option<ObjectStoreParams>> {
        let mut params = self.unwrap_or_default();
        if params.object_store_wrapper.is_some() {
            return Err(Error::Lance {
                message: "can not patch param because object store is already set".into(),
            });
        }
        params.object_store_wrapper = Some(wrapper);

        Ok(Some(params))
    }
}

pub trait PatchWriteParam {
    fn patch_with_store_wrapper(
        self,
        wrapper: Arc<dyn WrappingObjectStore>,
    ) -> Result<Option<WriteParams>>;
}

impl PatchWriteParam for Option<WriteParams> {
    fn patch_with_store_wrapper(
        self,
        wrapper: Arc<dyn WrappingObjectStore>,
    ) -> Result<Option<WriteParams>> {
        let mut params = self.unwrap_or_default();
        params.store_params = params.store_params.patch_with_store_wrapper(wrapper)?;
        Ok(Some(params))
    }
}

// NOTE: we have some API inconsistency here.
// WriteParam is found in the form of Option<WriteParam> and ReadParam is found in the form of ReadParam

pub trait PatchReadParam {
    fn patch_with_store_wrapper(self, wrapper: Arc<dyn WrappingObjectStore>) -> Result<ReadParams>;
}

impl PatchReadParam for ReadParams {
    fn patch_with_store_wrapper(
        mut self,
        wrapper: Arc<dyn WrappingObjectStore>,
    ) -> Result<ReadParams> {
        self.store_options = self.store_options.patch_with_store_wrapper(wrapper)?;
        Ok(self)
    }
}
