/*
 * LanceDB REST Catalog API
 *
 * REST interface for managing LanceDB databases and tables
 *
 * The version of the OpenAPI document: 0.0.1
 * 
 * Generated by: https://openapi-generator.tech
 */


use reqwest;
use serde::{Deserialize, Serialize, de::Error as _};
use crate::{apis::ResponseContent, models};
use super::{Error, configuration, ContentType};


/// struct for typed errors of method [`v1_databases_db_name_delete`]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum V1DatabasesDbNameDeleteError {
    UnknownValue(serde_json::Value),
}

/// struct for typed errors of method [`v1_databases_db_name_get`]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum V1DatabasesDbNameGetError {
    UnknownValue(serde_json::Value),
}

/// struct for typed errors of method [`v1_databases_db_name_put`]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum V1DatabasesDbNamePutError {
    UnknownValue(serde_json::Value),
}

/// struct for typed errors of method [`v1_databases_delete`]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum V1DatabasesDeleteError {
    UnknownValue(serde_json::Value),
}

/// struct for typed errors of method [`v1_databases_get`]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum V1DatabasesGetError {
    UnknownValue(serde_json::Value),
}

/// struct for typed errors of method [`v1_databases_post`]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum V1DatabasesPostError {
    UnknownValue(serde_json::Value),
}


pub async fn v1_databases_db_name_delete(configuration: &configuration::Configuration, db_name: &str) -> Result<(), Error<V1DatabasesDbNameDeleteError>> {
    // add a prefix to parameters to efficiently prevent name collisions
    let p_db_name = db_name;

    let uri_str = format!("{}/v1/databases/{dbName}", configuration.base_path, dbName=crate::apis::urlencode(p_db_name));
    let mut req_builder = configuration.client.request(reqwest::Method::DELETE, &uri_str);

    if let Some(ref user_agent) = configuration.user_agent {
        req_builder = req_builder.header(reqwest::header::USER_AGENT, user_agent.clone());
    }

    let req = req_builder.build()?;
    let resp = configuration.client.execute(req).await?;

    let status = resp.status();

    if !status.is_client_error() && !status.is_server_error() {
        Ok(())
    } else {
        let content = resp.text().await?;
        let entity: Option<V1DatabasesDbNameDeleteError> = serde_json::from_str(&content).ok();
        Err(Error::ResponseError(ResponseContent { status, content, entity }))
    }
}

pub async fn v1_databases_db_name_get(configuration: &configuration::Configuration, db_name: &str) -> Result<models::DatabaseMetadata, Error<V1DatabasesDbNameGetError>> {
    // add a prefix to parameters to efficiently prevent name collisions
    let p_db_name = db_name;

    let uri_str = format!("{}/v1/databases/{dbName}", configuration.base_path, dbName=crate::apis::urlencode(p_db_name));
    let mut req_builder = configuration.client.request(reqwest::Method::GET, &uri_str);

    if let Some(ref user_agent) = configuration.user_agent {
        req_builder = req_builder.header(reqwest::header::USER_AGENT, user_agent.clone());
    }

    let req = req_builder.build()?;
    let resp = configuration.client.execute(req).await?;

    let status = resp.status();
    let content_type = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/octet-stream");
    let content_type = super::ContentType::from(content_type);

    if !status.is_client_error() && !status.is_server_error() {
        let content = resp.text().await?;
        match content_type {
            ContentType::Json => serde_json::from_str(&content).map_err(Error::from),
            ContentType::Text => return Err(Error::from(serde_json::Error::custom("Received `text/plain` content type response that cannot be converted to `models::DatabaseMetadata`"))),
            ContentType::Unsupported(unknown_type) => return Err(Error::from(serde_json::Error::custom(format!("Received `{unknown_type}` content type response that cannot be converted to `models::DatabaseMetadata`")))),
        }
    } else {
        let content = resp.text().await?;
        let entity: Option<V1DatabasesDbNameGetError> = serde_json::from_str(&content).ok();
        Err(Error::ResponseError(ResponseContent { status, content, entity }))
    }
}

pub async fn v1_databases_db_name_put(configuration: &configuration::Configuration, db_name: &str, rename_request: models::RenameRequest) -> Result<(), Error<V1DatabasesDbNamePutError>> {
    // add a prefix to parameters to efficiently prevent name collisions
    let p_db_name = db_name;
    let p_rename_request = rename_request;

    let uri_str = format!("{}/v1/databases/{dbName}", configuration.base_path, dbName=crate::apis::urlencode(p_db_name));
    let mut req_builder = configuration.client.request(reqwest::Method::PUT, &uri_str);

    if let Some(ref user_agent) = configuration.user_agent {
        req_builder = req_builder.header(reqwest::header::USER_AGENT, user_agent.clone());
    }
    req_builder = req_builder.json(&p_rename_request);

    let req = req_builder.build()?;
    let resp = configuration.client.execute(req).await?;

    let status = resp.status();

    if !status.is_client_error() && !status.is_server_error() {
        Ok(())
    } else {
        let content = resp.text().await?;
        let entity: Option<V1DatabasesDbNamePutError> = serde_json::from_str(&content).ok();
        Err(Error::ResponseError(ResponseContent { status, content, entity }))
    }
}

pub async fn v1_databases_delete(configuration: &configuration::Configuration, ) -> Result<(), Error<V1DatabasesDeleteError>> {

    let uri_str = format!("{}/v1/databases", configuration.base_path);
    let mut req_builder = configuration.client.request(reqwest::Method::DELETE, &uri_str);

    if let Some(ref user_agent) = configuration.user_agent {
        req_builder = req_builder.header(reqwest::header::USER_AGENT, user_agent.clone());
    }

    let req = req_builder.build()?;
    let resp = configuration.client.execute(req).await?;

    let status = resp.status();

    if !status.is_client_error() && !status.is_server_error() {
        Ok(())
    } else {
        let content = resp.text().await?;
        let entity: Option<V1DatabasesDeleteError> = serde_json::from_str(&content).ok();
        Err(Error::ResponseError(ResponseContent { status, content, entity }))
    }
}

pub async fn v1_databases_get(configuration: &configuration::Configuration, start_after: Option<&str>, limit: Option<i32>) -> Result<models::DatabaseList, Error<V1DatabasesGetError>> {
    // add a prefix to parameters to efficiently prevent name collisions
    let p_start_after = start_after;
    let p_limit = limit;

    let uri_str = format!("{}/v1/databases", configuration.base_path);
    let mut req_builder = configuration.client.request(reqwest::Method::GET, &uri_str);

    if let Some(ref param_value) = p_start_after {
        req_builder = req_builder.query(&[("startAfter", &param_value.to_string())]);
    }
    if let Some(ref param_value) = p_limit {
        req_builder = req_builder.query(&[("limit", &param_value.to_string())]);
    }
    if let Some(ref user_agent) = configuration.user_agent {
        req_builder = req_builder.header(reqwest::header::USER_AGENT, user_agent.clone());
    }

    let req = req_builder.build()?;
    let resp = configuration.client.execute(req).await?;

    let status = resp.status();
    let content_type = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/octet-stream");
    let content_type = super::ContentType::from(content_type);

    if !status.is_client_error() && !status.is_server_error() {
        let content = resp.text().await?;
        match content_type {
            ContentType::Json => serde_json::from_str(&content).map_err(Error::from),
            ContentType::Text => return Err(Error::from(serde_json::Error::custom("Received `text/plain` content type response that cannot be converted to `models::DatabaseList`"))),
            ContentType::Unsupported(unknown_type) => return Err(Error::from(serde_json::Error::custom(format!("Received `{unknown_type}` content type response that cannot be converted to `models::DatabaseList`")))),
        }
    } else {
        let content = resp.text().await?;
        let entity: Option<V1DatabasesGetError> = serde_json::from_str(&content).ok();
        Err(Error::ResponseError(ResponseContent { status, content, entity }))
    }
}

pub async fn v1_databases_post(configuration: &configuration::Configuration, create_database_request: models::CreateDatabaseRequest) -> Result<(), Error<V1DatabasesPostError>> {
    // add a prefix to parameters to efficiently prevent name collisions
    let p_create_database_request = create_database_request;

    let uri_str = format!("{}/v1/databases", configuration.base_path);
    let mut req_builder = configuration.client.request(reqwest::Method::POST, &uri_str);

    if let Some(ref user_agent) = configuration.user_agent {
        req_builder = req_builder.header(reqwest::header::USER_AGENT, user_agent.clone());
    }
    req_builder = req_builder.json(&p_create_database_request);

    let req = req_builder.build()?;
    let resp = configuration.client.execute(req).await?;

    let status = resp.status();

    if !status.is_client_error() && !status.is_server_error() {
        Ok(())
    } else {
        let content = resp.text().await?;
        let entity: Option<V1DatabasesPostError> = serde_json::from_str(&content).ok();
        Err(Error::ResponseError(ResponseContent { status, content, entity }))
    }
}

