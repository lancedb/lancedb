/*
 * LanceDB REST Catalog API
 *
 * REST interface for managing LanceDB databases and tables
 *
 * The version of the OpenAPI document: 0.0.1
 * 
 * Generated by: https://openapi-generator.tech
 */

use crate::models;
use serde::{Deserialize, Serialize};

#[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize)]
pub struct V1DatabasesDbNameTablesTableNameNamePutRequest {
    #[serde(rename = "newName")]
    pub new_name: String,
}

impl V1DatabasesDbNameTablesTableNameNamePutRequest {
    pub fn new(new_name: String) -> V1DatabasesDbNameTablesTableNameNamePutRequest {
        V1DatabasesDbNameTablesTableNameNamePutRequest {
            new_name,
        }
    }
}

