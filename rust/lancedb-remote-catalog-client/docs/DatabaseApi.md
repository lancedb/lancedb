# \DatabaseApi

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**v1_databases_db_name_delete**](DatabaseApi.md#v1_databases_db_name_delete) | **DELETE** /v1/databases/{dbName} | Delete database
[**v1_databases_db_name_get**](DatabaseApi.md#v1_databases_db_name_get) | **GET** /v1/databases/{dbName} | Get database metadata
[**v1_databases_db_name_put**](DatabaseApi.md#v1_databases_db_name_put) | **PUT** /v1/databases/{dbName} | Rename database
[**v1_databases_delete**](DatabaseApi.md#v1_databases_delete) | **DELETE** /v1/databases | Delete all databases
[**v1_databases_get**](DatabaseApi.md#v1_databases_get) | **GET** /v1/databases | List databases
[**v1_databases_post**](DatabaseApi.md#v1_databases_post) | **POST** /v1/databases | Create new database



## v1_databases_db_name_delete

> v1_databases_db_name_delete(db_name)
Delete database

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


## v1_databases_db_name_get

> models::DatabaseMetadata v1_databases_db_name_get(db_name)
Get database metadata

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**db_name** | **String** |  | [required] |

### Return type

[**models::DatabaseMetadata**](DatabaseMetadata.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## v1_databases_db_name_put

> v1_databases_db_name_put(db_name, rename_request)
Rename database

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**db_name** | **String** |  | [required] |
**rename_request** | [**RenameRequest**](RenameRequest.md) |  | [required] |

### Return type

 (empty response body)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: Not defined

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## v1_databases_delete

> v1_databases_delete()
Delete all databases

### Parameters

This endpoint does not need any parameter.

### Return type

 (empty response body)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: Not defined

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## v1_databases_get

> models::DatabaseList v1_databases_get(start_after, limit)
List databases

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**start_after** | Option<**String**> |  |  |
**limit** | Option<**i32**> |  |  |

### Return type

[**models::DatabaseList**](DatabaseList.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## v1_databases_post

> v1_databases_post(create_database_request)
Create new database

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**create_database_request** | [**CreateDatabaseRequest**](CreateDatabaseRequest.md) |  | [required] |

### Return type

 (empty response body)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: Not defined

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

