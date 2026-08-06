// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use crate::dataset::refs::Branches;
use lance_core::{Error, Result};
use object_store::path::Path;

pub const BRANCH_DIR: &str = "tree";

#[derive(Debug, Clone, PartialEq, Hash)]
pub struct BranchLocation {
    pub path: Path,
    pub uri: String,
    pub branch: Option<String>,
}

impl BranchLocation {
    /// Find the root location
    pub fn find_main(&self) -> Result<Self> {
        if let Some(branch_name) = self.branch.as_deref() {
            let root_path_str = Self::get_root_path(self.path.as_ref(), branch_name)?;
            let root_uri = Self::get_root_path(self.uri.as_str(), branch_name)?;
            Ok(Self {
                path: Path::parse(root_path_str)?,
                uri: root_uri,
                branch: None,
            })
        } else {
            Ok(self.clone())
        }
    }

    fn get_root_path(path_str: &str, branch_name: &str) -> Result<String> {
        // A uri may carry a query string (e.g. `s3+ddb://...?ddbTableName=t`);
        // the branch suffix sits on the path part, before the query.
        let (path_part, query) = match path_str.split_once('?') {
            Some((path, query)) => (path, Some(query)),
            None => (path_str, None),
        };
        let branch_suffix = format!("{}/{}", BRANCH_DIR, branch_name);
        let branch_suffix = branch_suffix.as_str();
        let root_path_str = path_part
            .strip_suffix(branch_suffix)
            .or_else(|| {
                if cfg!(windows) {
                    let windows_suffix = branch_suffix.replace('/', "\\");
                    path_part.strip_suffix(&windows_suffix)
                } else {
                    None
                }
            })
            .ok_or_else(|| {
                Error::invalid_input(format!(
                    "Can not find the root location of branch {} by uri {}",
                    branch_name, path_str,
                ))
            })?;
        let root_path_str = if root_path_str.ends_with('/') {
            root_path_str.trim_end_matches('/').to_string()
        } else if cfg!(windows) {
            root_path_str.trim_end_matches('\\').to_string()
        } else {
            return Err(Error::invalid_input(format!(
                "Invalid dataset root uri {} for branch {}",
                root_path_str, path_str,
            )));
        };
        Ok(match query {
            Some(query) => format!("{}?{}", root_path_str, query),
            None => root_path_str,
        })
    }

    /// The branch a location under `root` targets: the inverse of
    /// [`Self::find_branch`]. `location` must be either `root` itself (main)
    /// or `<root>/tree/<branch>`; anything else is rejected so a caller never
    /// misattributes an unrelated location to a branch.
    pub fn branch_of(root: &str, location: &str) -> Result<Option<String>> {
        if location == root {
            return Ok(None);
        }
        // Require the `/` component boundary after the root so a sibling path
        // that merely shares the root as a string prefix is rejected.
        let branch = location
            .strip_prefix(root)
            .and_then(|rel| {
                if root.is_empty() {
                    Some(rel)
                } else {
                    rel.strip_prefix('/')
                }
            })
            .and_then(|rel| rel.strip_prefix(BRANCH_DIR))
            .and_then(|rel| rel.strip_prefix('/'))
            .filter(|name| !name.is_empty());

        match branch {
            Some(name) => Ok(Some(name.to_string())),
            None => Err(Error::invalid_input(format!(
                "cannot derive a branch for location '{}': expected the table root '{}' or a branch chain under '{}/{}'",
                location, root, root, BRANCH_DIR
            ))),
        }
    }

    /// Find the target branch location
    pub fn find_branch(&self, branch_name: Option<&str>) -> Result<Self> {
        if branch_name == self.branch.as_deref() {
            return Ok(self.clone());
        }

        let root_location = self.find_main()?;
        if Branches::is_main_branch(branch_name) {
            return Ok(root_location);
        }

        if let Some(target_branch) = branch_name {
            let (new_path, new_uri) = {
                // Handle empty segment
                if target_branch.is_empty() {
                    (self.path.clone(), self.uri.clone())
                } else {
                    let segments = target_branch.split('/');
                    let mut new_path_str = Self::join_str(root_location.path.as_ref(), "tree")?;
                    let mut new_uri = Self::join_str(root_location.uri.as_str(), "tree")?;
                    for segment in segments {
                        new_path_str = Self::join_str(new_path_str.as_str(), segment)?;
                        new_uri = Self::join_str(new_uri.as_str(), segment)?;
                    }
                    (Path::parse(new_path_str)?, new_uri)
                }
            };
            Ok(Self {
                path: new_path,
                uri: new_uri,
                branch: Some(target_branch.to_string()),
            })
        } else {
            Ok(root_location)
        }
    }

    fn join_str(base: &str, segment: &str) -> Result<String> {
        // A uri may carry a query string (e.g. `s3+ddb://...?ddbTableName=t`);
        // path segments must be appended before it.
        let (path_part, query) = match base.split_once('?') {
            Some((path, query)) => (path, Some(query)),
            None => (base, None),
        };
        let normalized_segment = segment.trim_start_matches('/');
        let is_base_dir = path_part.ends_with("/");
        let joined = if is_base_dir {
            format!("{}{}", path_part, normalized_segment)
        } else {
            format!("{}/{}", path_part, normalized_segment)
        };
        Ok(match query {
            Some(query) => format!("{}?{}", joined, query),
            None => joined,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::dataset::branch_location::BranchLocation;
    use lance_core::utils::tempfile::TempStdDir;
    use object_store::path::Path;
    use std::fs;
    use std::path::PathBuf;

    // Create a BranchLocation instance for testing
    fn create_branch_location(root_path: PathBuf) -> BranchLocation {
        let branch_dir = root_path.join("tree/feature/new");
        let test_uri = branch_dir.to_str().unwrap().to_string();
        BranchLocation {
            path: Path::parse(&test_uri).unwrap(),
            uri: test_uri,
            branch: Some("feature/new".to_string()),
        }
    }

    #[test]
    fn test_find_main_from_branch() {
        let root_path = TempStdDir::default().to_owned();
        let location = create_branch_location(root_path.clone());
        let main_location = location.find_main().unwrap();

        assert_eq!(
            main_location.path.as_ref(),
            Path::parse(root_path.to_str().unwrap()).unwrap().as_ref()
        );
        assert_eq!(main_location.uri, root_path.to_str().unwrap().to_string());
        assert_eq!(main_location.branch, None);
        assert!(fs::create_dir(std::path::Path::new(main_location.uri.as_str())).is_ok());
    }

    #[test]
    fn test_find_main_from_root() {
        let root_path = TempStdDir::default().to_owned();
        let mut location = create_branch_location(root_path);
        // Change current branch to Main
        location.branch = None;
        let root_location = location.find_main().unwrap();

        assert_eq!(root_location.path, location.path);
        assert_eq!(root_location.uri, location.uri);
        assert_eq!(root_location.branch, None);
        assert!(fs::create_dir_all(std::path::Path::new(root_location.uri.as_str())).is_ok());
    }

    #[test]
    fn test_find_branch_from_same_branch() {
        let root_path = TempStdDir::default().to_owned();
        let location = create_branch_location(root_path);
        let target_branch = location.branch.as_deref();
        let new_location = location.find_branch(target_branch).unwrap();

        assert_eq!(new_location.path, location.path);
        assert_eq!(new_location.uri, location.uri);
        assert_eq!(new_location.branch, location.branch);
        assert!(fs::create_dir_all(std::path::Path::new(new_location.uri.as_str())).is_ok());
    }

    #[test]
    fn test_find_main_branch() {
        let root_path = TempStdDir::default().to_owned();
        let location = create_branch_location(root_path);
        let main_location = location.find_branch(None).unwrap();

        let expected_root = location.find_main().unwrap();
        assert_eq!(main_location.path, expected_root.path);
        assert_eq!(main_location.uri, expected_root.uri);
        assert_eq!(main_location.branch, None);
        assert!(fs::create_dir_all(std::path::Path::new(main_location.uri.as_str())).is_ok());
    }

    #[test]
    fn test_find_simple_branch() {
        let root_path = TempStdDir::default().to_owned();
        let location = create_branch_location(root_path);
        let new_branch = Some("featureA");
        let main_location = location.find_main().unwrap();
        let new_location = location.find_branch(new_branch).unwrap();

        assert_eq!(
            new_location.path.as_ref(),
            format!("{}/tree/featureA", main_location.path.as_ref())
        );
        assert_eq!(
            new_location.uri,
            format!("{}/tree/featureA", main_location.uri)
        );
        assert_eq!(new_location.branch.as_deref(), new_branch);
        assert!(fs::create_dir_all(std::path::Path::new(new_location.uri.as_str())).is_ok());
    }

    #[test]
    fn test_find_complex_branch() {
        let root_path = TempStdDir::default().to_owned();
        let location = create_branch_location(root_path);
        let new_branch = Some("bugfix/issue-123");
        let main_location = location.find_main().unwrap();
        let new_location = location.find_branch(new_branch).unwrap();

        assert_eq!(
            new_location.path.as_ref(),
            format!("{}/tree/bugfix/issue-123", main_location.path.as_ref())
        );
        assert_eq!(
            new_location.uri,
            format!("{}/tree/bugfix/issue-123", main_location.uri)
        );
        assert!(fs::create_dir_all(std::path::Path::new(new_location.uri.as_str())).is_ok());
    }

    #[test]
    fn test_branch_location_with_query_uri() {
        // Uris like `s3+ddb://...?ddbTableName=t` carry the commit handler
        // config in the query string; branch path segments must be inserted
        // before it and the query must survive the round trip.
        let location = BranchLocation {
            path: Path::parse("bucket/table.lance").unwrap(),
            uri: "s3+ddb://bucket/table.lance?ddbTableName=t".to_string(),
            branch: None,
        };
        let dev = location.find_branch(Some("dev")).unwrap();
        assert_eq!(
            dev.uri,
            "s3+ddb://bucket/table.lance/tree/dev?ddbTableName=t"
        );
        assert_eq!(dev.path.as_ref(), "bucket/table.lance/tree/dev");
        assert_eq!(dev.branch.as_deref(), Some("dev"));

        let main = dev.find_main().unwrap();
        assert_eq!(main.uri, "s3+ddb://bucket/table.lance?ddbTableName=t");
        assert_eq!(main.path.as_ref(), "bucket/table.lance");
        assert_eq!(main.branch, None);
    }

    #[test]
    fn test_branch_of() {
        let derive = |root: &str, location: &str| BranchLocation::branch_of(root, location);

        // The table root targets main.
        assert_eq!(derive("data/t.lance", "data/t.lance").unwrap(), None);

        // Branch chains, including multi-segment branch names.
        assert_eq!(
            derive("data/t.lance", "data/t.lance/tree/exp").unwrap(),
            Some("exp".to_string())
        );
        assert_eq!(
            derive("data/t.lance", "data/t.lance/tree/bugfix/issue-123").unwrap(),
            Some("bugfix/issue-123".to_string())
        );

        // A sibling path sharing the root as a string prefix is not a branch.
        assert!(derive("data/t", "data/tx/tree/exp").is_err());
        // Neither is a sub-path outside the branch directory.
        assert!(derive("data/t.lance", "data/t.lance/other/exp").is_err());
        // Nor a path missing the component boundary after the branch dir.
        assert!(derive("data/t.lance", "data/t.lance/treex").is_err());
        // An empty branch name is invalid.
        assert!(derive("data/t.lance", "data/t.lance/tree").is_err());
        // An unrelated location is invalid.
        assert!(derive("data/t.lance", "elsewhere/u.lance").is_err());
    }

    #[test]
    fn test_find_empty_branch() {
        let root_path = TempStdDir::default().to_owned();
        let location = create_branch_location(root_path);
        let new_branch = Some("");
        let new_location = location.find_branch(new_branch).unwrap();

        assert_eq!(new_location.path, location.path);
        assert_eq!(new_location.uri, location.uri);
        assert_eq!(new_location.branch.as_deref(), new_branch);
    }

    #[test]
    #[cfg(windows)]
    fn test_branch_location_on_windows() {
        let branch_location = BranchLocation {
            path: Path::parse("C:\\Users\\Username\\Documents\\dataset\\tree\\feature\\new")
                .unwrap(),
            uri: "C:\\Users\\Username\\Documents\\dataset\\tree\\feature\\new".to_string(),
            branch: Some("feature/new".to_string()),
        };

        let main_location = branch_location.find_main().unwrap();
        assert_eq!(main_location.uri, "C:\\Users\\Username\\Documents\\dataset");
        assert_eq!(
            main_location.path.as_ref(),
            Path::parse("C:\\Users\\Username\\Documents\\dataset")
                .unwrap()
                .as_ref()
        );
        assert_eq!(main_location.branch, None);

        let new_branch = branch_location
            .find_branch(Some("feature/nathan/A"))
            .unwrap();
        assert_eq!(
            new_branch.uri,
            "C:\\Users\\Username\\Documents\\dataset/tree/feature/nathan/A"
        );
        assert_eq!(
            new_branch.path.as_ref(),
            Path::parse("C:\\Users\\Username\\Documents\\dataset/tree/feature/nathan/A")
                .unwrap()
                .as_ref()
        );
        assert_eq!(new_branch.branch.as_deref(), Some("feature/nathan/A"));
    }
}
