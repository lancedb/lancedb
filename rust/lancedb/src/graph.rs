// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Property graphs: tables read as the nodes and edges of a graph.
//!
//! A property graph is a catalog object in a namespace, next to the tables it
//! reads. Its definition says which tables hold nodes and which hold edges:
//! each node table's key and label, each edge table's label, and how its
//! source and destination columns reference node keys -- the shape of SQL/PGQ's
//! `CREATE PROPERTY GRAPH`. The tables themselves stay ordinary tables.
//!
//! The verbs live on [`crate::connection::Connection`].

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

/// A table whose rows are the nodes of one label.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeTable {
    /// The table, in the graph's namespace.
    pub table: String,
    /// The column that identifies a node.
    pub key: String,
    /// The label its nodes carry.
    pub label: String,
    /// The columns exposed as node properties; every column when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub properties: Option<Vec<String>>,
}

/// A table whose rows are the edges of one label.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EdgeTable {
    /// The table, in the graph's namespace.
    pub table: String,
    /// The label its edges carry.
    pub label: String,
    /// The column naming each edge's source node.
    pub source: Endpoint,
    /// The column naming each edge's destination node.
    pub destination: Endpoint,
    /// The columns exposed as edge properties; every column when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub properties: Option<Vec<String>>,
}

/// One end of an edge: a column of the edge table and the node key it holds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Endpoint {
    /// The edge table's column.
    pub column: String,
    /// The node table and key column its values match.
    pub references: EndpointReference,
}

/// The node table and key column an endpoint's values match.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EndpointReference {
    /// A node table of the same graph.
    pub table: String,
    /// That node table's key column.
    pub column: String,
}

/// The node and edge tables that make up a property graph.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PropertyGraphDefinition {
    /// The node tables, one per label.
    pub nodes: Vec<NodeTable>,
    /// The edge tables, one per label.
    #[serde(default)]
    pub edges: Vec<EdgeTable>,
}

impl PropertyGraphDefinition {
    /// Parse a definition from its JSON form.
    pub fn from_json(json: &str) -> Result<Self> {
        serde_json::from_str(json).map_err(|source| Error::InvalidInput {
            message: format!("invalid property graph definition: {source}"),
        })
    }
}

/// The version of one table a graph was built from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphSourceVersion {
    /// The table.
    pub table: String,
    /// Its version when the graph was built.
    pub version: u64,
}

/// What a database records about one property graph.
///
/// Returned by [`crate::connection::Connection::describe_property_graph`], and
/// by `create_property_graph`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PropertyGraphDescription {
    /// The graph's name within its namespace.
    pub name: String,
    /// The namespace holding the graph; empty is the root namespace.
    #[serde(rename = "namespace", default)]
    pub namespace_path: Vec<String>,
    /// The node tables.
    pub nodes: Vec<NodeTable>,
    /// The edge tables.
    #[serde(default)]
    pub edges: Vec<EdgeTable>,
    /// How many nodes the graph held when it was built.
    pub vertex_count: u64,
    /// How many edges the graph held when it was built.
    pub edge_count: u64,
    /// The table versions the graph was built from.
    #[serde(default)]
    pub sources: Vec<GraphSourceVersion>,
}

impl PropertyGraphDescription {
    /// The description in its JSON form.
    pub fn to_json(&self) -> Result<String> {
        serde_json::to_string(self).map_err(|source| Error::Runtime {
            message: format!("could not encode a property graph description: {source}"),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn social() -> PropertyGraphDefinition {
        let person = EndpointReference {
            table: "person".to_string(),
            column: "person_id".to_string(),
        };
        PropertyGraphDefinition {
            nodes: vec![NodeTable {
                table: "person".to_string(),
                key: "person_id".to_string(),
                label: "Person".to_string(),
                properties: Some(vec!["name".to_string()]),
            }],
            edges: vec![EdgeTable {
                table: "knows".to_string(),
                label: "KNOWS".to_string(),
                source: Endpoint {
                    column: "src_id".to_string(),
                    references: person.clone(),
                },
                destination: Endpoint {
                    column: "dst_id".to_string(),
                    references: person,
                },
                properties: None,
            }],
        }
    }

    #[test]
    fn test_definition_json_is_the_sql_pgq_shape() {
        let json = serde_json::to_value(social()).unwrap();
        assert_eq!(
            json,
            serde_json::json!({
                "nodes": [{"table": "person", "key": "person_id", "label": "Person",
                           "properties": ["name"]}],
                "edges": [{"table": "knows", "label": "KNOWS",
                           "source": {"column": "src_id",
                                      "references": {"table": "person", "column": "person_id"}},
                           "destination": {"column": "dst_id",
                                           "references": {"table": "person",
                                                          "column": "person_id"}}}]
            })
        );
        assert_eq!(
            PropertyGraphDefinition::from_json(&json.to_string()).unwrap(),
            social()
        );
        assert!(PropertyGraphDefinition::from_json(r#"{"edges": []}"#).is_err());
    }

    #[tokio::test]
    async fn test_local_databases_refuse_property_graphs() {
        let dir = tempfile::tempdir().unwrap();
        let conn = crate::connect(dir.path().to_str().unwrap())
            .execute()
            .await
            .unwrap();
        let refused = |error: Error| {
            assert!(
                matches!(error, Error::NotSupported { ref message }
                    if message.contains("Property graph operations")),
                "{error}"
            );
        };
        refused(
            conn.create_property_graph("social", &social(), &[])
                .await
                .unwrap_err(),
        );
        refused(
            conn.describe_property_graph("social", &[])
                .await
                .unwrap_err(),
        );
        refused(conn.drop_property_graph("social", &[]).await.unwrap_err());
        refused(conn.list_property_graphs(&[]).await.unwrap_err());
    }
}
