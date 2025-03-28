# \TableApi

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**v1_databases_db_name_tables_delete**](TableApi.md#v1_databases_db_name_tables_delete) | **DELETE** /v1/databases/{dbName}/tables | Delete all tables
[**v1_databases_db_name_tables_get**](TableApi.md#v1_databases_db_name_tables_get) | **GET** /v1/databases/{dbName}/tables | List tables in database
[**v1_databases_db_name_tables_post**](TableApi.md#v1_databases_db_name_tables_post) | **POST** /v1/databases/{dbName}/tables | Create new table
[**v1_databases_db_name_tables_table_name_delete**](TableApi.md#v1_databases_db_name_tables_table_name_delete) | **DELETE** /v1/databases/{dbName}/tables/{tableName} | Delete a specific table
[**v1_databases_db_name_tables_table_name_get**](TableApi.md#v1_databases_db_name_tables_table_name_get) | **GET** /v1/databases/{dbName}/tables/{tableName} | Get table metadata
[**v1_databases_db_name_tables_table_name_name_put**](TableApi.md#v1_databases_db_name_tables_table_name_name_put) | **PUT** /v1/databases/{dbName}/tables/{tableName}/name | Rename table
[**v1_databases_db_name_tables_table_name_put**](TableApi.md#v1_databases_db_name_tables_table_name_put) | **PUT** /v1/databases/{dbName}/tables/{tableName} | Update table data



## v1_databases_db_name_tables_delete

> v1_databases_db_name_tables_delete(db_name)
Delete all tables

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**db_name** | **String** |  | [required] |

### Return type

 (empty response body)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: Not defined

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## v1_databases_db_name_tables_get

> models::TableList v1_databases_db_name_tables_get(db_name, start_after, limit)
List tables in database

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**db_name** | **String** |  | [required] |
**start_after** | Option<**String**> |  |  |
**limit** | Option<**i32**> |  |  |

### Return type

[**models::TableList**](TableList.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## v1_databases_db_name_tables_post

> v1_databases_db_name_tables_post(db_name, create_table_request)
Create new table

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**db_name** | **String** |  | [required] |
**create_table_request** | [**CreateTableRequest**](CreateTableRequest.md) |  | [required] |

### Return type

 (empty response body)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: Not defined

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## v1_databases_db_name_tables_table_name_delete

> v1_databases_db_name_tables_table_name_delete(db_name, table_name)
Delete a specific table

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**db_name** | **String** |  | [required] |
**table_name** | **String** |  | [required] |

### Return type

 (empty response body)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: Not defined

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## v1_databases_db_name_tables_table_name_get

> models::TableMetadata v1_databases_db_name_tables_table_name_get(db_name, table_name)
Get table metadata

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**db_name** | **String** |  | [required] |
**table_name** | **String** |  | [required] |

### Return type

[**models::TableMetadata**](TableMetadata.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## v1_databases_db_name_tables_table_name_name_put

> v1_databases_db_name_tables_table_name_name_put(db_name, table_name, v1_databases_db_name_tables_table_name_name_put_request)
Rename table

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**db_name** | **String** |  | [required] |
**table_name** | **String** |  | [required] |
**v1_databases_db_name_tables_table_name_name_put_request** | Option<[**V1DatabasesDbNameTablesTableNameNamePutRequest**](V1DatabasesDbNameTablesTableNameNamePutRequest.md)> |  |  |

### Return type

 (empty response body)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: Not defined

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## v1_databases_db_name_tables_table_name_put

> v1_databases_db_name_tables_table_name_put(db_name, table_name, table_update)
Update table data

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**db_name** | **String** |  | [required] |
**table_name** | **String** |  | [required] |
**table_update** | Option<[**TableUpdate**](TableUpdate.md)> |  |  |

### Return type

 (empty response body)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: Not defined

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

