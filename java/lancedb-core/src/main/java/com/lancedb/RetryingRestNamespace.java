/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.lancedb;

import org.apache.arrow.memory.BufferAllocator;
import org.lance.namespace.DynamicContextProvider;
import org.lance.namespace.RestNamespace;
import org.lance.namespace.model.AlterTableAddColumnsRequest;
import org.lance.namespace.model.AlterTableAddColumnsResponse;
import org.lance.namespace.model.AlterTableAlterColumnsRequest;
import org.lance.namespace.model.AlterTableAlterColumnsResponse;
import org.lance.namespace.model.AlterTableDropColumnsRequest;
import org.lance.namespace.model.AlterTableDropColumnsResponse;
import org.lance.namespace.model.AlterTransactionRequest;
import org.lance.namespace.model.AlterTransactionResponse;
import org.lance.namespace.model.AnalyzeTableQueryPlanRequest;
import org.lance.namespace.model.BatchDeleteTableVersionsRequest;
import org.lance.namespace.model.BatchDeleteTableVersionsResponse;
import org.lance.namespace.model.CountTableRowsRequest;
import org.lance.namespace.model.CountTableRowsResponse;
import org.lance.namespace.model.CreateMaterializedViewRequest;
import org.lance.namespace.model.CreateMaterializedViewResponse;
import org.lance.namespace.model.CreateNamespaceRequest;
import org.lance.namespace.model.CreateNamespaceResponse;
import org.lance.namespace.model.CreateTableIndexRequest;
import org.lance.namespace.model.CreateTableIndexResponse;
import org.lance.namespace.model.CreateTableRequest;
import org.lance.namespace.model.CreateTableResponse;
import org.lance.namespace.model.CreateTableScalarIndexResponse;
import org.lance.namespace.model.CreateTableTagRequest;
import org.lance.namespace.model.CreateTableTagResponse;
import org.lance.namespace.model.CreateTableVersionRequest;
import org.lance.namespace.model.CreateTableVersionResponse;
import org.lance.namespace.model.DeclareTableRequest;
import org.lance.namespace.model.DeclareTableResponse;
import org.lance.namespace.model.DeleteFromTableRequest;
import org.lance.namespace.model.DeleteFromTableResponse;
import org.lance.namespace.model.DeleteTableTagRequest;
import org.lance.namespace.model.DeleteTableTagResponse;
import org.lance.namespace.model.DeregisterTableRequest;
import org.lance.namespace.model.DeregisterTableResponse;
import org.lance.namespace.model.DescribeNamespaceRequest;
import org.lance.namespace.model.DescribeNamespaceResponse;
import org.lance.namespace.model.DescribeTableIndexStatsRequest;
import org.lance.namespace.model.DescribeTableIndexStatsResponse;
import org.lance.namespace.model.DescribeTableRequest;
import org.lance.namespace.model.DescribeTableResponse;
import org.lance.namespace.model.DescribeTableVersionRequest;
import org.lance.namespace.model.DescribeTableVersionResponse;
import org.lance.namespace.model.DescribeTransactionRequest;
import org.lance.namespace.model.DescribeTransactionResponse;
import org.lance.namespace.model.DropNamespaceRequest;
import org.lance.namespace.model.DropNamespaceResponse;
import org.lance.namespace.model.DropTableIndexRequest;
import org.lance.namespace.model.DropTableIndexResponse;
import org.lance.namespace.model.DropTableRequest;
import org.lance.namespace.model.DropTableResponse;
import org.lance.namespace.model.ExplainTableQueryPlanRequest;
import org.lance.namespace.model.GetTableStatsRequest;
import org.lance.namespace.model.GetTableStatsResponse;
import org.lance.namespace.model.GetTableTagVersionRequest;
import org.lance.namespace.model.GetTableTagVersionResponse;
import org.lance.namespace.model.InsertIntoTableRequest;
import org.lance.namespace.model.InsertIntoTableResponse;
import org.lance.namespace.model.ListNamespacesRequest;
import org.lance.namespace.model.ListNamespacesResponse;
import org.lance.namespace.model.ListTableIndicesRequest;
import org.lance.namespace.model.ListTableIndicesResponse;
import org.lance.namespace.model.ListTableTagsRequest;
import org.lance.namespace.model.ListTableTagsResponse;
import org.lance.namespace.model.ListTableVersionsRequest;
import org.lance.namespace.model.ListTableVersionsResponse;
import org.lance.namespace.model.ListTablesRequest;
import org.lance.namespace.model.ListTablesResponse;
import org.lance.namespace.model.MergeInsertIntoTableRequest;
import org.lance.namespace.model.MergeInsertIntoTableResponse;
import org.lance.namespace.model.NamespaceExistsRequest;
import org.lance.namespace.model.NamespaceExistsResponse;
import org.lance.namespace.model.QueryTableRequest;
import org.lance.namespace.model.QueryTableResponse;
import org.lance.namespace.model.RegisterTableRequest;
import org.lance.namespace.model.RegisterTableResponse;
import org.lance.namespace.model.RenameTableRequest;
import org.lance.namespace.model.RenameTableResponse;
import org.lance.namespace.model.RestoreTableRequest;
import org.lance.namespace.model.RestoreTableResponse;
import org.lance.namespace.model.TableExistsRequest;
import org.lance.namespace.model.TableExistsResponse;
import org.lance.namespace.model.UpdateTableRequest;
import org.lance.namespace.model.UpdateTableResponse;
import org.lance.namespace.model.UpdateTableSchemaMetadataRequest;
import org.lance.namespace.model.UpdateTableSchemaMetadataResponse;
import org.lance.namespace.model.UpdateTableTagRequest;
import org.lance.namespace.model.UpdateTableTagResponse;

import java.util.Map;
import java.util.concurrent.Callable;

/**
 * {@link RestNamespace} that retries throttled calls made on this object.
 *
 * <p>Extending {@link RestNamespace} keeps {@code Dataset.open} on the native handle, which does
 * not pass through these overrides. Credential refresh and namespace commits on that path are not
 * retried. Calls made on the client {@link LanceDbNamespaceClientBuilder#build()} returns are.
 *
 * <p>This instance is never initialized. {@code initialize} would open a second native client, and
 * the real handle stays on {@code delegate}. {@link #getNativeHandle()} forwards that handle. Every
 * public instance method {@link RestNamespace} declares is overridden for the same reason: an
 * inherited method would see a zero handle and throw "not initialized" instead of reaching the
 * delegate. Reads retry throttling and service unavailable. Writes retry throttling only. The
 * budget is four attempts, with base waits of 250 ms, 500 ms, and 1000 ms, plus up to 250 ms of
 * jitter.
 */
final class RetryingRestNamespace extends RestNamespace {
  private final RestNamespace delegate;

  RetryingRestNamespace(RestNamespace delegate) {
    if (delegate == null) {
      throw new IllegalArgumentException("Namespace cannot be null");
    }
    this.delegate = delegate;
  }

  @Override
  public void initialize(Map<String, String> configProperties, BufferAllocator allocator) {
    delegate.initialize(configProperties, allocator);
  }

  @Override
  public void initialize(
      Map<String, String> configProperties,
      BufferAllocator allocator,
      DynamicContextProvider contextProvider) {
    delegate.initialize(configProperties, allocator, contextProvider);
  }

  @Override
  public String namespaceId() {
    return delegate.namespaceId();
  }

  @Override
  public long getNativeHandle() {
    return delegate.getNativeHandle();
  }

  @Override
  public Map<String, Long> retrieveOpsMetrics() {
    return delegate.retrieveOpsMetrics();
  }

  @Override
  public void resetOpsMetrics() {
    delegate.resetOpsMetrics();
  }

  @Override
  public void close() {
    delegate.close();
  }

  @Override
  public ListNamespacesResponse listNamespaces(ListNamespacesRequest request) {
    return read(() -> delegate.listNamespaces(request));
  }

  @Override
  public DescribeNamespaceResponse describeNamespace(DescribeNamespaceRequest request) {
    return read(() -> delegate.describeNamespace(request));
  }

  @Override
  public CreateNamespaceResponse createNamespace(CreateNamespaceRequest request) {
    return write(() -> delegate.createNamespace(request));
  }

  @Override
  public DropNamespaceResponse dropNamespace(DropNamespaceRequest request) {
    return write(() -> delegate.dropNamespace(request));
  }

  @Override
  public NamespaceExistsResponse namespaceExists(NamespaceExistsRequest request) {
    return read(() -> delegate.namespaceExists(request));
  }

  @Override
  public ListTablesResponse listTables(ListTablesRequest request) {
    return read(() -> delegate.listTables(request));
  }

  @Override
  public DescribeTableResponse describeTable(DescribeTableRequest request) {
    return read(() -> delegate.describeTable(request));
  }

  @Override
  public RegisterTableResponse registerTable(RegisterTableRequest request) {
    return write(() -> delegate.registerTable(request));
  }

  @Override
  public TableExistsResponse tableExists(TableExistsRequest request) {
    return read(() -> delegate.tableExists(request));
  }

  @Override
  public DropTableResponse dropTable(DropTableRequest request) {
    return write(() -> delegate.dropTable(request));
  }

  @Override
  public DeregisterTableResponse deregisterTable(DeregisterTableRequest request) {
    return write(() -> delegate.deregisterTable(request));
  }

  @Override
  public CountTableRowsResponse countTableRows(CountTableRowsRequest request) {
    return read(() -> delegate.countTableRows(request));
  }

  @Override
  public CreateTableResponse createTable(CreateTableRequest request, byte[] requestData) {
    return write(() -> delegate.createTable(request, requestData));
  }

  @Override
  public DeclareTableResponse declareTable(DeclareTableRequest request) {
    return write(() -> delegate.declareTable(request));
  }

  @Override
  public RenameTableResponse renameTable(RenameTableRequest request) {
    return write(() -> delegate.renameTable(request));
  }

  @Override
  public InsertIntoTableResponse insertIntoTable(
      InsertIntoTableRequest request, byte[] requestData) {
    return write(() -> delegate.insertIntoTable(request, requestData));
  }

  @Override
  public MergeInsertIntoTableResponse mergeInsertIntoTable(
      MergeInsertIntoTableRequest request, byte[] requestData) {
    return write(() -> delegate.mergeInsertIntoTable(request, requestData));
  }

  @Override
  public UpdateTableResponse updateTable(UpdateTableRequest request) {
    return write(() -> delegate.updateTable(request));
  }

  @Override
  public DeleteFromTableResponse deleteFromTable(DeleteFromTableRequest request) {
    return write(() -> delegate.deleteFromTable(request));
  }

  @Override
  public QueryTableResponse queryTable(QueryTableRequest request) {
    return read(() -> delegate.queryTable(request));
  }

  @Override
  public CreateTableIndexResponse createTableIndex(CreateTableIndexRequest request) {
    return write(() -> delegate.createTableIndex(request));
  }

  @Override
  public ListTableIndicesResponse listTableIndices(ListTableIndicesRequest request) {
    return read(() -> delegate.listTableIndices(request));
  }

  @Override
  public DescribeTableIndexStatsResponse describeTableIndexStats(
      DescribeTableIndexStatsRequest request, String indexName) {
    return read(() -> delegate.describeTableIndexStats(request, indexName));
  }

  @Override
  public DescribeTransactionResponse describeTransaction(DescribeTransactionRequest request) {
    return read(() -> delegate.describeTransaction(request));
  }

  @Override
  public AlterTransactionResponse alterTransaction(AlterTransactionRequest request) {
    return write(() -> delegate.alterTransaction(request));
  }

  @Override
  public ListTableVersionsResponse listTableVersions(ListTableVersionsRequest request) {
    return read(() -> delegate.listTableVersions(request));
  }

  @Override
  public CreateTableVersionResponse createTableVersion(CreateTableVersionRequest request) {
    return write(() -> delegate.createTableVersion(request));
  }

  @Override
  public DescribeTableVersionResponse describeTableVersion(DescribeTableVersionRequest request) {
    return read(() -> delegate.describeTableVersion(request));
  }

  @Override
  public BatchDeleteTableVersionsResponse batchDeleteTableVersions(
      BatchDeleteTableVersionsRequest request) {
    return write(() -> delegate.batchDeleteTableVersions(request));
  }

  @Override
  public CreateTableScalarIndexResponse createTableScalarIndex(CreateTableIndexRequest request) {
    return write(() -> delegate.createTableScalarIndex(request));
  }

  @Override
  public DropTableIndexResponse dropTableIndex(DropTableIndexRequest request, String indexName) {
    return write(() -> delegate.dropTableIndex(request, indexName));
  }

  @Override
  public ListTablesResponse listAllTables(ListTablesRequest request) {
    return read(() -> delegate.listAllTables(request));
  }

  @Override
  public RestoreTableResponse restoreTable(RestoreTableRequest request) {
    return write(() -> delegate.restoreTable(request));
  }

  @Override
  public UpdateTableSchemaMetadataResponse updateTableSchemaMetadata(
      UpdateTableSchemaMetadataRequest request) {
    return write(() -> delegate.updateTableSchemaMetadata(request));
  }

  @Override
  public GetTableStatsResponse getTableStats(GetTableStatsRequest request) {
    return read(() -> delegate.getTableStats(request));
  }

  @Override
  public String explainTableQueryPlan(ExplainTableQueryPlanRequest request) {
    return read(() -> delegate.explainTableQueryPlan(request));
  }

  @Override
  public String analyzeTableQueryPlan(AnalyzeTableQueryPlanRequest request) {
    return read(() -> delegate.analyzeTableQueryPlan(request));
  }

  @Override
  public AlterTableAddColumnsResponse alterTableAddColumns(AlterTableAddColumnsRequest request) {
    return write(() -> delegate.alterTableAddColumns(request));
  }

  @Override
  public AlterTableAlterColumnsResponse alterTableAlterColumns(
      AlterTableAlterColumnsRequest request) {
    return write(() -> delegate.alterTableAlterColumns(request));
  }

  @Override
  public AlterTableDropColumnsResponse alterTableDropColumns(AlterTableDropColumnsRequest request) {
    return write(() -> delegate.alterTableDropColumns(request));
  }

  @Override
  public ListTableTagsResponse listTableTags(ListTableTagsRequest request) {
    return read(() -> delegate.listTableTags(request));
  }

  @Override
  public GetTableTagVersionResponse getTableTagVersion(GetTableTagVersionRequest request) {
    return read(() -> delegate.getTableTagVersion(request));
  }

  @Override
  public CreateTableTagResponse createTableTag(CreateTableTagRequest request) {
    return write(() -> delegate.createTableTag(request));
  }

  @Override
  public DeleteTableTagResponse deleteTableTag(DeleteTableTagRequest request) {
    return write(() -> delegate.deleteTableTag(request));
  }

  @Override
  public UpdateTableTagResponse updateTableTag(UpdateTableTagRequest request) {
    return write(() -> delegate.updateTableTag(request));
  }

  @Override
  public CreateMaterializedViewResponse createMaterializedView(
      CreateMaterializedViewRequest request) {
    return write(() -> delegate.createMaterializedView(request));
  }

  private static <T> T read(Callable<T> action) {
    return ThrottleRetry.call(action, true);
  }

  private static <T> T write(Callable<T> action) {
    return ThrottleRetry.call(action, false);
  }
}
