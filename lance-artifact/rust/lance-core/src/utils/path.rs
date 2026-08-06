// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use object_store::path::Path;

pub trait LancePathExt {
    fn child_path(&self, path: &Path) -> Path;
}

impl LancePathExt for Path {
    fn child_path(&self, path: &Path) -> Path {
        let mut new_path = self.clone();
        for part in path.parts() {
            new_path = new_path.clone().join(part);
        }
        new_path
    }
}
