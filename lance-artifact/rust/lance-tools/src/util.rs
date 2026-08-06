// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use lance_core::{Error, Result};
use lance_io::object_store::{ObjectStore, ObjectStoreParams, ObjectStoreRegistry};
use object_store::path::Path;
use std::sync::Arc;
use url::Url;

fn path_to_parent(path: &Path) -> Result<(Path, String)> {
    let mut parts = path.parts().collect::<Vec<_>>();
    if parts.is_empty() {
        return Err(Error::invalid_input(format!(
            "Path {} is not a valid path to a file",
            path
        )));
    }
    let filename = parts.pop().unwrap().as_ref().to_owned();
    Ok((Path::from_iter(parts), filename))
}

/// Get an object store and a path from a source string.
pub(crate) async fn get_object_store_and_path(source: &String) -> Result<(Arc<ObjectStore>, Path)> {
    if let Ok(mut url) = Url::parse(source)
        && url.scheme().len() > 1
    {
        let path = object_store::path::Path::parse(url.path()).map_err(Error::from)?;
        let (parent_path, filename) = path_to_parent(&path)?;
        url.set_path(parent_path.as_ref());
        let object_store_registry = Arc::new(ObjectStoreRegistry::default());
        let object_store_params = ObjectStoreParams::default();
        let (object_store, dir_path) = ObjectStore::from_uri_and_params(
            object_store_registry,
            url.as_str(),
            &object_store_params,
        )
        .await?;
        let child_path = dir_path.clone().join(filename);
        return Ok((object_store, child_path));
    }
    let path = Path::from_filesystem_path(source)?;
    let object_store = Arc::new(ObjectStore::local());
    Ok((object_store, path))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_path_to_parent() {
        let (parent_path, filename) =
            path_to_parent(&object_store::path::Path::parse("/a/b/c").unwrap()).unwrap();
        assert_eq!("c", filename);
        let parts: Vec<_> = parent_path.parts().collect();
        assert_eq!(2, parts.len());
        assert_eq!("a", parts.first().unwrap().as_ref());
        assert_eq!("b", parts.get(1).unwrap().as_ref());
    }
}
