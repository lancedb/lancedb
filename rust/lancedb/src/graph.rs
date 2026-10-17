// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Property graphs: a graph defined over tables a database already holds.
//!
//! A property graph stores no rows. Its definition says which tables of its
//! namespace hold nodes and which hold edges -- each node table's key and
//! label, each edge table's label and the node keys its source and destination
//! columns reference -- the shape of SQL/PGQ's `CREATE PROPERTY GRAPH`. A query
//! reads the tables through SQL `GRAPH_TABLE`, as they are when it runs.
//!
//! The verbs live on [`crate::connection::Connection`]. A graph is read
//! through SQL, by name, so there is no handle returning its rows.

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

/// A property graph's node and edge tables.
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

/// A table whose rows are the nodes of one label.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeTable {
    /// The table, in the graph's namespace.
    pub table: String,
    /// The column that identifies a node.
    pub key: String,
    /// The label its nodes carry.
    pub label: String,
    /// The columns exposed as node properties; every column when absent. A
    /// created graph reports the list it resolved to, the key among them.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub properties: Option<Vec<String>>,
}

impl NodeTable {
    /// The nodes labeled `label` are the rows of `table`, each identified by
    /// its `key` column, with every column a property.
    pub fn new(table: impl Into<String>, key: impl Into<String>, label: impl Into<String>) -> Self {
        Self {
            table: table.into(),
            key: key.into(),
            label: label.into(),
            properties: None,
        }
    }

    /// Expose only these columns as node properties.
    pub fn properties(mut self, properties: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.properties = Some(properties.into_iter().map(Into::into).collect());
        self
    }
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
    /// The columns exposed as edge properties; every column when absent. A
    /// created graph reports the list it resolved to, both endpoint columns
    /// among them.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub properties: Option<Vec<String>>,
}

impl EdgeTable {
    /// The edges labeled `label` are the rows of `table`, running from the
    /// node `source` names to the node `destination` names, with every column
    /// a property.
    pub fn new(
        table: impl Into<String>,
        label: impl Into<String>,
        source: Endpoint,
        destination: Endpoint,
    ) -> Self {
        Self {
            table: table.into(),
            label: label.into(),
            source,
            destination,
            properties: None,
        }
    }

    /// Expose only these columns as edge properties.
    pub fn properties(mut self, properties: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.properties = Some(properties.into_iter().map(Into::into).collect());
        self
    }
}

/// One end of an edge: a column of the edge table and the node key it holds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Endpoint {
    /// The edge table's column.
    pub column: String,
    /// The node table and key column its values match.
    pub references: EndpointReference,
}

impl Endpoint {
    /// The edge table's `column`, holding keys of the node table `table`,
    /// whose key column is `key`.
    pub fn new(
        column: impl Into<String>,
        table: impl Into<String>,
        key: impl Into<String>,
    ) -> Self {
        Self {
            column: column.into(),
            references: EndpointReference {
                table: table.into(),
                column: key.into(),
            },
        }
    }
}

/// The node table and key column an endpoint's values match.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EndpointReference {
    /// A node table of the same graph.
    pub table: String,
    /// That node table's key column.
    pub column: String,
}

/// What a database records about one property graph: its node and edge
/// tables, with each table's properties as create resolved them.
///
/// Returned by [`crate::connection::Connection::describe_property_graph`], and
/// by `create_property_graph` so a caller has the resolved properties without
/// a second call.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PropertyGraphDescription {
    /// The graph's name within its namespace.
    pub name: String,
    /// The namespace holding the graph; empty is the root namespace.
    #[serde(rename = "namespace", default)]
    pub namespace_path: Vec<String>,
    /// The graph's node and edge tables.
    #[serde(flatten)]
    pub definition: PropertyGraphDefinition,
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
    use futures::TryStreamExt;

    use super::*;

    fn social() -> PropertyGraphDefinition {
        PropertyGraphDefinition {
            nodes: vec![NodeTable::new("person", "person_id", "Person").properties(["name"])],
            edges: vec![EdgeTable::new(
                "knows",
                "KNOWS",
                Endpoint::new("src_id", "person", "person_id"),
                Endpoint::new("dst_id", "person", "person_id"),
            )],
        }
    }

    #[test]
    fn test_a_definition_is_the_sql_pgq_shape() {
        let json = serde_json::to_value(social()).unwrap();
        // Properties left out are left out of the body too: absent means every
        // column, which an empty list would not.
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
        assert_eq!(
            PropertyGraphDefinition::from_json(r#"{"nodes": []}"#).unwrap(),
            PropertyGraphDefinition {
                nodes: vec![],
                edges: vec![],
            }
        );
        assert!(matches!(
            PropertyGraphDefinition::from_json(r#"{"edges": []}"#),
            Err(Error::InvalidInput { .. })
        ));
    }

    #[test]
    fn test_a_description_carries_its_definition_at_the_top_level() {
        let description = PropertyGraphDescription {
            name: "social".to_string(),
            namespace_path: vec!["analytics".to_string()],
            definition: social(),
        };
        let json: serde_json::Value =
            serde_json::from_str(&description.to_json().unwrap()).unwrap();
        assert_eq!(json["name"], "social");
        assert_eq!(json["namespace"], serde_json::json!(["analytics"]));
        assert_eq!(json["nodes"][0]["table"], "person");
        assert_eq!(json["edges"][0]["label"], "KNOWS");
        let decoded: PropertyGraphDescription = serde_json::from_value(json).unwrap();
        assert_eq!(decoded, description);
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
        refused(
            conn.list_property_graphs(&[], Default::default())
                .try_collect::<Vec<_>>()
                .await
                .unwrap_err(),
        );
    }
}
