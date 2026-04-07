// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Arrow Flight SQL server for LanceDB.
//!
//! This module provides an Arrow Flight SQL server that exposes a LanceDB
//! [`Connection`] over the Flight SQL protocol. Any ADBC, ODBC (via bridge),
//! or JDBC Flight SQL client can connect and run SQL queries — including
//! LanceDB's search table functions (`vector_search`, `fts`, `hybrid_search`).
//!
//! # Quick Start
//!
//! ```no_run
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let db = lancedb::connect("data/my-db").execute().await?;
//! let addr = "0.0.0.0:50051".parse()?;
//! lancedb::flight::serve(db, addr).await?;
//! # Ok(())
//! # }
//! ```

use std::collections::HashMap;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, StringArray};
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::FlightServiceServer;
use arrow_flight::sql::server::FlightSqlService;
use arrow_flight::sql::{
    Any, CommandGetCatalogs, CommandGetDbSchemas, CommandGetTableTypes, CommandGetTables,
    CommandStatementQuery, SqlInfo, TicketStatementQuery,
};
use arrow_flight::{FlightDescriptor, FlightEndpoint, FlightInfo, Ticket};
use arrow_schema::{DataType, Field, Schema as ArrowSchema, SchemaRef};
use datafusion::prelude::SessionContext;
use datafusion_catalog::TableProvider;
use datafusion_common::{DataFusionError, Result as DataFusionResult};
use futures::StreamExt;
use futures::stream;
use log;
use prost::Message;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

use crate::connection::Connection;
use crate::table::datafusion::BaseTableAdapter;
use crate::table::datafusion::udtf::fts::FtsTableFunction;
use crate::table::datafusion::udtf::hybrid_search::HybridSearchTableFunction;
use crate::table::datafusion::udtf::vector_search::VectorSearchTableFunction;
use crate::table::datafusion::udtf::{SearchQuery, TableResolver};

/// Start an Arrow Flight SQL server exposing the given LanceDB connection.
///
/// This is a convenience function that creates a server and starts listening.
/// It blocks until the server is shut down.
pub async fn serve(connection: Connection, addr: SocketAddr) -> crate::Result<()> {
    let service = LanceFlightSqlService::try_new(connection).await?;
    let flight_svc = FlightServiceServer::new(service);

    Server::builder()
        .add_service(flight_svc)
        .serve(addr)
        .await
        .map_err(|e| crate::Error::Runtime {
            message: format!("Flight SQL server error: {}", e),
        })?;

    Ok(())
}

/// A table resolver that looks up tables from a pre-built HashMap.
#[derive(Debug)]
struct ConnectionTableResolver {
    tables: HashMap<String, Arc<BaseTableAdapter>>,
}

impl TableResolver for ConnectionTableResolver {
    fn resolve_table(
        &self,
        name: &str,
        search: Option<SearchQuery>,
    ) -> DataFusionResult<Arc<dyn TableProvider>> {
        let adapter = self
            .tables
            .get(name)
            .ok_or_else(|| DataFusionError::Plan(format!("Table '{}' not found", name)))?;

        match search {
            None => Ok(adapter.clone() as Arc<dyn TableProvider>),
            Some(SearchQuery::Fts(fts)) => Ok(Arc::new(adapter.with_fts_query(fts))),
            Some(SearchQuery::Vector(vq)) => Ok(Arc::new(adapter.with_vector_query(vq))),
            Some(SearchQuery::Hybrid { fts, vector }) => {
                Ok(Arc::new(adapter.with_hybrid_query(fts, vector)))
            }
        }
    }
}

/// Arrow Flight SQL service backed by a LanceDB connection.
struct LanceFlightSqlService {
    /// Kept for future use (e.g., refreshing table list).
    _connection: Connection,
    /// Pre-built table adapters (refreshed on creation)
    tables: HashMap<String, Arc<BaseTableAdapter>>,
}

impl LanceFlightSqlService {
    async fn try_new(connection: Connection) -> crate::Result<Self> {
        let table_names = connection.table_names().execute().await?;
        let mut tables = HashMap::new();

        for name in &table_names {
            let table = connection.open_table(name).execute().await?;
            let adapter = BaseTableAdapter::try_new(table.base_table().clone()).await?;
            tables.insert(name.clone(), Arc::new(adapter));
        }

        log::info!(
            "LanceDB Flight SQL service initialized with {} tables",
            tables.len()
        );

        Ok(Self {
            _connection: connection,
            tables,
        })
    }

    /// Create a DataFusion SessionContext with all tables and UDTFs registered.
    fn create_session_context(&self) -> SessionContext {
        let ctx = SessionContext::new();

        // Register all tables
        for (name, adapter) in &self.tables {
            if let Err(e) = ctx.register_table(name, adapter.clone() as Arc<dyn TableProvider>) {
                log::warn!("Failed to register table '{}': {}", name, e);
            }
        }

        // Create resolver for UDTFs
        let resolver = Arc::new(ConnectionTableResolver {
            tables: self.tables.clone(),
        });

        // Register search UDTFs
        ctx.register_udtf("fts", Arc::new(FtsTableFunction::new(resolver.clone())));
        ctx.register_udtf(
            "vector_search",
            Arc::new(VectorSearchTableFunction::new(resolver.clone())),
        );
        ctx.register_udtf(
            "hybrid_search",
            Arc::new(HybridSearchTableFunction::new(resolver)),
        );

        ctx
    }

    /// Execute a SQL query and return the results as a stream of FlightData.
    async fn execute_sql(
        &self,
        sql: &str,
    ) -> Result<
        Pin<Box<dyn futures::Stream<Item = Result<arrow_flight::FlightData, Status>> + Send>>,
        Status,
    > {
        let ctx = self.create_session_context();

        let df = ctx
            .sql(sql)
            .await
            .map_err(|e| Status::internal(format!("SQL planning error: {}", e)))?;

        let schema: SchemaRef = df.schema().inner().clone();
        let stream = df
            .execute_stream()
            .await
            .map_err(|e| Status::internal(format!("SQL execution error: {}", e)))?;

        // Use FlightDataEncoderBuilder to properly encode batches with schema
        let batch_stream = stream.map(|r| r.map_err(|e| FlightError::ExternalError(Box::new(e))));
        let flight_data_stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(batch_stream)
            .map(|result| result.map_err(|e| Status::internal(format!("Encoding error: {}", e))));

        Ok(Box::pin(flight_data_stream))
    }

    /// Encode a single RecordBatch into a FlightData stream (schema + data).
    fn batch_to_flight_stream(
        batch: RecordBatch,
    ) -> Pin<Box<dyn futures::Stream<Item = Result<arrow_flight::FlightData, Status>> + Send>> {
        let schema = batch.schema();
        let stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(stream::once(async move { Ok(batch) }))
            .map(|result| result.map_err(|e| Status::internal(format!("Encoding error: {}", e))));
        Box::pin(stream)
    }

    /// Get the schema for a SQL query without executing it.
    async fn get_sql_schema(&self, sql: &str) -> Result<ArrowSchema, Status> {
        let ctx = self.create_session_context();
        let df = ctx
            .sql(sql)
            .await
            .map_err(|e| Status::internal(format!("SQL planning error: {}", e)))?;
        Ok(df.schema().inner().as_ref().clone())
    }
}

#[tonic::async_trait]
impl FlightSqlService for LanceFlightSqlService {
    type FlightService = LanceFlightSqlService;

    /// Handle SQL query: return FlightInfo with schema and ticket.
    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let sql = query.query;
        log::info!("get_flight_info_statement: {}", sql);

        let schema = self.get_sql_schema(&sql).await?;

        // Encode the query as an Any-wrapped TicketStatementQuery for do_get_statement
        let ticket = TicketStatementQuery {
            statement_handle: sql.into_bytes().into(),
        };
        let any_msg = Any::pack(&ticket)
            .map_err(|e| Status::internal(format!("Ticket encoding error: {}", e)))?;
        let mut ticket_bytes = Vec::new();
        any_msg
            .encode(&mut ticket_bytes)
            .map_err(|e| Status::internal(format!("Ticket encoding error: {}", e)))?;

        let endpoint = FlightEndpoint::new().with_ticket(Ticket::new(ticket_bytes));
        let flight_info = FlightInfo::new()
            .try_with_schema(&schema)
            .map_err(|e| Status::internal(format!("Schema error: {}", e)))?
            .with_endpoint(endpoint)
            .with_descriptor(request.into_inner());

        Ok(Response::new(flight_info))
    }

    /// Execute a SQL query and stream results.
    async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
        _request: Request<Ticket>,
    ) -> Result<
        Response<<Self as arrow_flight::flight_service_server::FlightService>::DoGetStream>,
        Status,
    > {
        let sql = String::from_utf8(ticket.statement_handle.to_vec())
            .map_err(|e| Status::internal(format!("Invalid ticket: {}", e)))?;
        log::info!("do_get_statement: {}", sql);

        let stream = self.execute_sql(&sql).await?;
        Ok(Response::new(stream))
    }

    /// List tables in the database.
    async fn get_flight_info_tables(
        &self,
        _query: CommandGetTables,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let schema = ArrowSchema::new(vec![
            Field::new("catalog_name", DataType::Utf8, true),
            Field::new("db_schema_name", DataType::Utf8, true),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("table_type", DataType::Utf8, false),
        ]);

        let cmd = CommandGetTables::default();
        let any_msg =
            Any::pack(&cmd).map_err(|e| Status::internal(format!("Encoding error: {}", e)))?;
        let mut ticket_bytes = Vec::new();
        any_msg
            .encode(&mut ticket_bytes)
            .map_err(|e| Status::internal(format!("Encoding error: {}", e)))?;

        let endpoint = FlightEndpoint::new().with_ticket(Ticket::new(ticket_bytes));
        let flight_info = FlightInfo::new()
            .try_with_schema(&schema)
            .map_err(|e| Status::internal(format!("Schema error: {}", e)))?
            .with_endpoint(endpoint)
            .with_descriptor(request.into_inner());

        Ok(Response::new(flight_info))
    }

    async fn do_get_tables(
        &self,
        _query: CommandGetTables,
        _request: Request<Ticket>,
    ) -> Result<
        Response<<Self as arrow_flight::flight_service_server::FlightService>::DoGetStream>,
        Status,
    > {
        let table_names: Vec<&str> = self.tables.keys().map(|s| s.as_str()).collect();
        let num_tables = table_names.len();

        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("catalog_name", DataType::Utf8, true),
            Field::new("db_schema_name", DataType::Utf8, true),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("table_type", DataType::Utf8, false),
        ]));

        let catalog_names: ArrayRef =
            Arc::new(StringArray::from(vec![Some("lancedb"); num_tables]));
        let schema_names: ArrayRef = Arc::new(StringArray::from(vec![Some("default"); num_tables]));
        let table_name_array: ArrayRef = Arc::new(StringArray::from(table_names));
        let table_types: ArrayRef = Arc::new(StringArray::from(vec!["TABLE"; num_tables]));

        let batch = RecordBatch::try_new(
            schema,
            vec![catalog_names, schema_names, table_name_array, table_types],
        )
        .map_err(|e| Status::internal(format!("RecordBatch error: {}", e)))?;

        Ok(Response::new(Self::batch_to_flight_stream(batch)))
    }

    /// List table types.
    async fn get_flight_info_table_types(
        &self,
        _query: CommandGetTableTypes,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let schema = ArrowSchema::new(vec![Field::new("table_type", DataType::Utf8, false)]);

        let cmd = CommandGetTableTypes::default();
        let any_msg =
            Any::pack(&cmd).map_err(|e| Status::internal(format!("Encoding error: {}", e)))?;
        let mut ticket_bytes = Vec::new();
        any_msg
            .encode(&mut ticket_bytes)
            .map_err(|e| Status::internal(format!("Encoding error: {}", e)))?;

        let endpoint = FlightEndpoint::new().with_ticket(Ticket::new(ticket_bytes));
        let flight_info = FlightInfo::new()
            .try_with_schema(&schema)
            .map_err(|e| Status::internal(format!("Schema error: {}", e)))?
            .with_endpoint(endpoint)
            .with_descriptor(request.into_inner());

        Ok(Response::new(flight_info))
    }

    async fn do_get_table_types(
        &self,
        _query: CommandGetTableTypes,
        _request: Request<Ticket>,
    ) -> Result<
        Response<<Self as arrow_flight::flight_service_server::FlightService>::DoGetStream>,
        Status,
    > {
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "table_type",
            DataType::Utf8,
            false,
        )]));
        let table_types: ArrayRef = Arc::new(StringArray::from(vec!["TABLE"]));
        let batch = RecordBatch::try_new(schema, vec![table_types])
            .map_err(|e| Status::internal(format!("RecordBatch error: {}", e)))?;

        Ok(Response::new(Self::batch_to_flight_stream(batch)))
    }

    /// List catalogs.
    async fn get_flight_info_catalogs(
        &self,
        _query: CommandGetCatalogs,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let schema = ArrowSchema::new(vec![Field::new("catalog_name", DataType::Utf8, false)]);

        let cmd = CommandGetCatalogs::default();
        let any_msg =
            Any::pack(&cmd).map_err(|e| Status::internal(format!("Encoding error: {}", e)))?;
        let mut ticket_bytes = Vec::new();
        any_msg
            .encode(&mut ticket_bytes)
            .map_err(|e| Status::internal(format!("Encoding error: {}", e)))?;

        let endpoint = FlightEndpoint::new().with_ticket(Ticket::new(ticket_bytes));
        let flight_info = FlightInfo::new()
            .try_with_schema(&schema)
            .map_err(|e| Status::internal(format!("Schema error: {}", e)))?
            .with_endpoint(endpoint)
            .with_descriptor(request.into_inner());

        Ok(Response::new(flight_info))
    }

    async fn do_get_catalogs(
        &self,
        _query: CommandGetCatalogs,
        _request: Request<Ticket>,
    ) -> Result<
        Response<<Self as arrow_flight::flight_service_server::FlightService>::DoGetStream>,
        Status,
    > {
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "catalog_name",
            DataType::Utf8,
            false,
        )]));
        let catalogs: ArrayRef = Arc::new(StringArray::from(vec!["lancedb"]));
        let batch = RecordBatch::try_new(schema, vec![catalogs])
            .map_err(|e| Status::internal(format!("RecordBatch error: {}", e)))?;

        Ok(Response::new(Self::batch_to_flight_stream(batch)))
    }

    /// List schemas.
    async fn get_flight_info_schemas(
        &self,
        _query: CommandGetDbSchemas,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let schema = ArrowSchema::new(vec![
            Field::new("catalog_name", DataType::Utf8, true),
            Field::new("db_schema_name", DataType::Utf8, false),
        ]);

        let cmd = CommandGetDbSchemas::default();
        let any_msg =
            Any::pack(&cmd).map_err(|e| Status::internal(format!("Encoding error: {}", e)))?;
        let mut ticket_bytes = Vec::new();
        any_msg
            .encode(&mut ticket_bytes)
            .map_err(|e| Status::internal(format!("Encoding error: {}", e)))?;

        let endpoint = FlightEndpoint::new().with_ticket(Ticket::new(ticket_bytes));
        let flight_info = FlightInfo::new()
            .try_with_schema(&schema)
            .map_err(|e| Status::internal(format!("Schema error: {}", e)))?
            .with_endpoint(endpoint)
            .with_descriptor(request.into_inner());

        Ok(Response::new(flight_info))
    }

    async fn do_get_schemas(
        &self,
        _query: CommandGetDbSchemas,
        _request: Request<Ticket>,
    ) -> Result<
        Response<<Self as arrow_flight::flight_service_server::FlightService>::DoGetStream>,
        Status,
    > {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("catalog_name", DataType::Utf8, true),
            Field::new("db_schema_name", DataType::Utf8, false),
        ]));
        let catalogs: ArrayRef = Arc::new(StringArray::from(vec![Some("lancedb")]));
        let schemas: ArrayRef = Arc::new(StringArray::from(vec!["default"]));
        let batch = RecordBatch::try_new(schema, vec![catalogs, schemas])
            .map_err(|e| Status::internal(format!("RecordBatch error: {}", e)))?;

        Ok(Response::new(Self::batch_to_flight_stream(batch)))
    }

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Float32Array, Int32Array};
    use arrow_flight::sql::client::FlightSqlServiceClient;
    use futures::TryStreamExt;
    use lance_arrow::FixedSizeListArrayExt;
    use std::time::Duration;

    async fn create_test_db() -> Connection {
        let db = crate::connect("memory://flight_test")
            .execute()
            .await
            .unwrap();

        let dim = 4i32;
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("text", DataType::Utf8, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), dim),
                true,
            ),
        ]));

        let ids = Int32Array::from(vec![1, 2, 3]);
        let texts = StringArray::from(vec!["hello world", "foo bar", "baz qux"]);
        let flat_values = Float32Array::from(vec![
            1.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0,
        ]);
        let vectors =
            arrow_array::FixedSizeListArray::try_new_from_values(flat_values, dim).unwrap();

        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(ids), Arc::new(texts), Arc::new(vectors)],
        )
        .unwrap();

        let table = db
            .create_table("test_table", batch)
            .execute()
            .await
            .unwrap();

        // Create FTS index
        table
            .create_index(&["text"], crate::index::Index::FTS(Default::default()))
            .execute()
            .await
            .unwrap();

        db
    }

    #[tokio::test]
    async fn test_flight_sql_basic_query() {
        let db = create_test_db().await;
        let service = LanceFlightSqlService::try_new(db).await.unwrap();
        let flight_svc = FlightServiceServer::new(service);

        // Start server on a random port
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server_handle = tokio::spawn(async move {
            Server::builder()
                .add_service(flight_svc)
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });

        // Give server a moment to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Connect client
        let channel = tonic::transport::Channel::from_shared(format!("http://{}", addr))
            .unwrap()
            .connect()
            .await
            .unwrap();
        let mut client = FlightSqlServiceClient::new(channel);

        // Execute SQL query
        let flight_info = client
            .execute("SELECT id, text FROM test_table".to_string(), None)
            .await
            .unwrap();

        // Fetch results using the FlightSql client's do_get
        let ticket = flight_info.endpoint[0].ticket.as_ref().unwrap().clone();
        let flight_stream = client.do_get(ticket).await.unwrap();

        let batches: Vec<RecordBatch> = flight_stream.try_collect().await.unwrap();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3, "Should return all 3 rows");

        // Verify schema
        if let Some(batch) = batches.first() {
            assert!(batch.schema().column_with_name("id").is_some());
            assert!(batch.schema().column_with_name("text").is_some());
        }

        server_handle.abort();
    }

    #[tokio::test]
    async fn test_flight_sql_table_listing() {
        let db = create_test_db().await;
        let service = LanceFlightSqlService::try_new(db).await.unwrap();

        assert!(service.tables.contains_key("test_table"));
        assert_eq!(service.tables.len(), 1);
    }

    #[tokio::test]
    async fn test_flight_sql_session_context() {
        let db = create_test_db().await;
        let service = LanceFlightSqlService::try_new(db).await.unwrap();
        let ctx = service.create_session_context();

        // Test that we can execute a simple query
        let df = ctx.sql("SELECT * FROM test_table LIMIT 1").await.unwrap();
        let results = df.collect().await.unwrap();
        assert_eq!(results[0].num_rows(), 1);
    }
}
