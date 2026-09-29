// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Property graphs: catalog objects read as graphs, in three kinds that share
//! one namespace of names.
//!
//! - A property graph holds its own rows. Its node and edge types have
//!   schemas, and rows are inserted into the graph itself.
//! - A virtual property graph reads tables in its namespace -- which hold
//!   nodes and which hold edges, each node table's key and label, each edge
//!   table's label and how its source and destination columns reference node
//!   keys, the shape of SQL/PGQ's `CREATE PROPERTY GRAPH` -- as they are when
//!   a query reads them.
//! - A materialized virtual property graph has a virtual graph's definition,
//!   and is read as its last refresh from the tables built it.
//!
//! The verbs live on [`crate::connection::Connection`].

use arrow_schema::Schema;
use lance_namespace::models::JsonArrowSchema;
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

fn from_json<T: serde::de::DeserializeOwned>(json: &str) -> Result<T> {
    serde_json::from_str(json).map_err(|source| Error::InvalidInput {
        message: format!("invalid property graph definition: {source}"),
    })
}

fn to_json(value: &impl Serialize) -> Result<String> {
    serde_json::to_string(value).map_err(|source| Error::Runtime {
        message: format!("could not encode a property graph description: {source}"),
    })
}

/// `schema` in the JSON Arrow form a graph definition carries.
///
/// The form spells no timestamp or duration unit, no time zone and no
/// dictionary, and cannot spell a time or an interval at all, so a schema that
/// would not read back as itself is refused here rather than reaching the
/// server as another schema.
pub fn schema_to_json(schema: &Schema) -> Result<JsonArrowSchema> {
    let json = lance_namespace::schema::arrow_schema_to_json(schema)?;
    for (field, encoded) in schema.fields().iter().zip(&json.fields) {
        let read_back = lance_namespace::schema::convert_json_arrow_field(encoded).ok();
        if read_back.as_ref() != Some(field.as_ref()) {
            let as_read = match read_back {
                Some(read_back) => format!("it would be read as {}", read_back.data_type()),
                None => "it cannot be read back".to_string(),
            };
            return Err(Error::InvalidInput {
                message: format!(
                    "property '{}' of type {} cannot be carried by a property graph \
                     definition: {as_read}",
                    field.name(),
                    field.data_type()
                ),
            });
        }
    }
    Ok(json)
}

/// A property graph's node and edge types.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PropertyGraphDefinition {
    /// The node types, one per label.
    pub nodes: Vec<NodeType>,
    /// The edge types, one per label.
    #[serde(default)]
    pub edges: Vec<EdgeType>,
}

impl PropertyGraphDefinition {
    /// Parse a definition from its JSON form.
    pub fn from_json(json: &str) -> Result<Self> {
        from_json(json)
    }
}

/// The node and edge tables of a virtual property graph, which a materialized
/// virtual property graph shares.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VirtualPropertyGraphDefinition {
    /// The node tables, one per label.
    pub nodes: Vec<NodeTable>,
    /// The edge tables, one per label.
    #[serde(default)]
    pub edges: Vec<EdgeTable>,
}

impl VirtualPropertyGraphDefinition {
    /// Parse a definition from its JSON form.
    pub fn from_json(json: &str) -> Result<Self> {
        from_json(json)
    }
}

/// The nodes of one label of a property graph.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodeType {
    /// The label its nodes carry.
    pub label: String,
    /// The field that identifies a node: a non-nullable integer, string or
    /// binary field of `schema`.
    pub key: String,
    /// Every property, the key among them.
    pub schema: JsonArrowSchema,
}

impl NodeType {
    pub fn new(label: impl Into<String>, key: impl Into<String>, schema: &Schema) -> Result<Self> {
        Ok(Self {
            label: label.into(),
            key: key.into(),
            schema: schema_to_json(schema)?,
        })
    }

    /// The properties as an Arrow schema.
    pub fn arrow_schema(&self) -> Result<Schema> {
        Ok(lance_namespace::schema::convert_json_arrow_schema(
            &self.schema,
        )?)
    }
}

/// The edges of one label of a property graph.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EdgeType {
    /// The label its edges carry.
    pub label: String,
    /// The label of each edge's source node, and the column inserted rows
    /// carry its key in.
    pub source: EndpointType,
    /// The label of each edge's destination node, and the column inserted
    /// rows carry its key in.
    pub destination: EndpointType,
    /// The edge's properties, when it has any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<JsonArrowSchema>,
}

impl EdgeType {
    pub fn new(
        label: impl Into<String>,
        source: EndpointType,
        destination: EndpointType,
        schema: Option<&Schema>,
    ) -> Result<Self> {
        Ok(Self {
            label: label.into(),
            source,
            destination,
            schema: schema.map(schema_to_json).transpose()?,
        })
    }

    /// The properties as an Arrow schema; empty when there are none.
    pub fn arrow_schema(&self) -> Result<Schema> {
        match &self.schema {
            Some(schema) => Ok(lance_namespace::schema::convert_json_arrow_schema(schema)?),
            None => Ok(Schema::empty()),
        }
    }
}

/// One end of an edge type.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EndpointType {
    /// The label of the node at this end.
    pub label: String,
    /// The column of inserted edge rows holding that node's key.
    pub column: String,
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
    /// created graph reports the list it resolved to.
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
    /// The columns exposed as edge properties; every column when absent. A
    /// created graph reports the list it resolved to.
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

/// The version of one table a materialized graph's last refresh read.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphSourceVersion {
    /// The table.
    pub table: String,
    /// Its version when the graph was refreshed.
    pub version: u64,
}

/// What a database records about one property graph.
///
/// Returned by [`crate::connection::Connection::describe_property_graph`], and
/// by every call that writes one.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PropertyGraphDescription {
    /// The graph's name within its namespace.
    pub name: String,
    /// The namespace holding the graph; empty is the root namespace.
    #[serde(rename = "namespace", default)]
    pub namespace_path: Vec<String>,
    /// The graph's node and edge types.
    #[serde(flatten)]
    pub definition: PropertyGraphDefinition,
    /// The graph's current commit.
    pub commit: String,
    /// When the current commit landed, as RFC 3339.
    pub committed_at: String,
    /// The commit a rollback returns to.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_commit: Option<String>,
    /// How many nodes the current commit holds.
    pub vertex_count: u64,
    /// How many edges the current commit holds.
    pub edge_count: u64,
}

impl PropertyGraphDescription {
    /// The description in its JSON form.
    pub fn to_json(&self) -> Result<String> {
        to_json(self)
    }
}

/// What a database records about one virtual property graph: its node and
/// edge tables, with each table's properties as create resolved them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VirtualPropertyGraphDescription {
    /// The graph's name within its namespace.
    pub name: String,
    /// The namespace holding the graph; empty is the root namespace.
    #[serde(rename = "namespace", default)]
    pub namespace_path: Vec<String>,
    /// The graph's node and edge tables.
    #[serde(flatten)]
    pub definition: VirtualPropertyGraphDefinition,
}

impl VirtualPropertyGraphDescription {
    /// The description in its JSON form.
    pub fn to_json(&self) -> Result<String> {
        to_json(self)
    }
}

/// What a database records about one materialized virtual property graph:
/// its node and edge tables, and what its last refresh built from them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MaterializedVirtualPropertyGraphDescription {
    /// The graph's name within its namespace.
    pub name: String,
    /// The namespace holding the graph; empty is the root namespace.
    #[serde(rename = "namespace", default)]
    pub namespace_path: Vec<String>,
    /// The graph's node and edge tables.
    #[serde(flatten)]
    pub definition: VirtualPropertyGraphDefinition,
    /// The last refresh's commit.
    pub commit: String,
    /// When the last refresh landed, as RFC 3339.
    pub committed_at: String,
    /// How many nodes the last refresh built.
    pub vertex_count: u64,
    /// How many edges the last refresh built.
    pub edge_count: u64,
    /// The version of each table the last refresh read.
    #[serde(default)]
    pub sources: Vec<GraphSourceVersion>,
}

impl MaterializedVirtualPropertyGraphDescription {
    /// The description in its JSON form.
    pub fn to_json(&self) -> Result<String> {
        to_json(self)
    }
}

#[cfg(test)]
mod tests {
    use arrow_schema::{DataType, Field, TimeUnit};

    use super::*;

    fn social() -> VirtualPropertyGraphDefinition {
        let person = EndpointReference {
            table: "person".to_string(),
            column: "person_id".to_string(),
        };
        VirtualPropertyGraphDefinition {
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
    fn test_a_virtual_definition_is_the_sql_pgq_shape() {
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
            VirtualPropertyGraphDefinition::from_json(&json.to_string()).unwrap(),
            social()
        );
        assert!(VirtualPropertyGraphDefinition::from_json(r#"{"edges": []}"#).is_err());
    }

    #[test]
    fn test_a_property_graph_definition_carries_its_schemas() {
        let person = NodeType::new(
            "Person",
            "person_id",
            &Schema::new(vec![
                Field::new("person_id", DataType::Int64, false),
                Field::new("tags", DataType::new_list(DataType::Utf8, true), true),
            ]),
        )
        .unwrap();
        let knows = EdgeType::new(
            "KNOWS",
            EndpointType {
                label: "Person".to_string(),
                column: "src_id".to_string(),
            },
            EndpointType {
                label: "Person".to_string(),
                column: "dst_id".to_string(),
            },
            None,
        )
        .unwrap();
        let definition = PropertyGraphDefinition {
            nodes: vec![person.clone()],
            edges: vec![knows],
        };
        let json = serde_json::to_value(&definition).unwrap();
        assert!(json.get("mode").is_none());
        assert_eq!(json["nodes"][0]["schema"]["fields"][0]["name"], "person_id");
        assert!(json["edges"][0].get("schema").is_none());
        assert_eq!(
            PropertyGraphDefinition::from_json(&json.to_string()).unwrap(),
            definition
        );
        assert_eq!(
            person.arrow_schema().unwrap().field(1).data_type(),
            &DataType::new_list(DataType::Utf8, true)
        );
    }

    #[test]
    fn test_a_materialized_description_carries_its_definition_at_the_top_level() {
        let description = MaterializedVirtualPropertyGraphDescription {
            name: "social".to_string(),
            namespace_path: vec!["analytics".to_string()],
            definition: social(),
            commit: "c1".to_string(),
            committed_at: "2026-09-26T17:00:00Z".to_string(),
            vertex_count: 4,
            edge_count: 5,
            sources: vec![GraphSourceVersion {
                table: "person".to_string(),
                version: 3,
            }],
        };
        let json = serde_json::to_value(&description).unwrap();
        assert_eq!(json["namespace"], serde_json::json!(["analytics"]));
        assert_eq!(json["nodes"][0]["table"], "person");
        let decoded: MaterializedVirtualPropertyGraphDescription =
            serde_json::from_value(json).unwrap();
        assert_eq!(decoded, description);
    }

    /// A type the JSON form would read back as another type, or not at all,
    /// is refused before any request carries it, in node and edge schemas.
    #[test]
    fn test_a_schema_the_definition_cannot_carry_is_refused() {
        let endpoint = |column: &str| EndpointType {
            label: "Person".to_string(),
            column: column.to_string(),
        };
        for data_type in [
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            DataType::Timestamp(TimeUnit::Second, None),
            DataType::Duration(TimeUnit::Nanosecond),
            DataType::Time32(TimeUnit::Second),
            DataType::Time64(TimeUnit::Microsecond),
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            DataType::new_list(DataType::Timestamp(TimeUnit::Millisecond, None), true),
        ] {
            let schema = Schema::new(vec![
                Field::new("person_id", DataType::Int64, false),
                Field::new("at", data_type.clone(), true),
            ]);
            let error = NodeType::new("Person", "person_id", &schema)
                .unwrap_err()
                .to_string();
            assert!(error.contains("property 'at'"), "{data_type}: {error}");
            let error = EdgeType::new("KNOWS", endpoint("src"), endpoint("dst"), Some(&schema))
                .unwrap_err()
                .to_string();
            assert!(error.contains("property 'at'"), "{data_type}: {error}");
        }

        // What the form spells exactly still round-trips.
        let schema = Schema::new(vec![
            Field::new("person_id", DataType::Int64, false),
            Field::new("at", DataType::Timestamp(TimeUnit::Microsecond, None), true),
            Field::new("took", DataType::Duration(TimeUnit::Microsecond), true),
            Field::new("price", DataType::Decimal128(18, 4), true),
        ]);
        let person = NodeType::new("Person", "person_id", &schema).unwrap();
        assert_eq!(person.arrow_schema().unwrap(), schema);
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
        let people = PropertyGraphDefinition {
            nodes: vec![],
            edges: vec![],
        };
        refused(
            conn.create_property_graph("people", &people, &[])
                .await
                .unwrap_err(),
        );
        refused(
            conn.create_virtual_property_graph("social", &social(), &[])
                .await
                .unwrap_err(),
        );
        refused(
            conn.create_materialized_virtual_property_graph("social", &social(), &[])
                .await
                .unwrap_err(),
        );
        refused(
            conn.describe_virtual_property_graph("social", &[])
                .await
                .unwrap_err(),
        );
        refused(
            conn.refresh_materialized_virtual_property_graph("social", &[])
                .await
                .unwrap_err(),
        );
        refused(
            conn.rollback_property_graph("people", &[])
                .await
                .unwrap_err(),
        );
        refused(conn.drop_property_graph("people", &[]).await.unwrap_err());
        refused(
            conn.list_materialized_virtual_property_graphs(&[])
                .await
                .unwrap_err(),
        );
    }
}
