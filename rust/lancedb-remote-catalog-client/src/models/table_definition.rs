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
pub struct TableDefinition {
    #[serde(rename = "column_definitions", skip_serializing_if = "Option::is_none")]
    pub column_definitions: Option<serde_json::Value>,
    #[serde(rename = "schema", skip_serializing_if = "Option::is_none")]
    pub schema: Option<serde_json::Value>,
}

impl TableDefinition {
    pub fn new() -> TableDefinition {
        TableDefinition {
            column_definitions: None,
            schema: None,
        }
    }
}

