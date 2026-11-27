// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use super::client::{HttpSend, RequestResultExt, RestfulLanceDbClient, Sender};
use crate::Result;
use crate::authz::*;
use serde::{Serialize, de::DeserializeOwned};

pub struct RemoteAuthorization<S: HttpSend = Sender> {
    pub(crate) client: RestfulLanceDbClient<S>,
}

impl<S: HttpSend> std::fmt::Debug for RemoteAuthorization<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteAuthorization")
            .finish_non_exhaustive()
    }
}

impl<S: HttpSend> RemoteAuthorization<S> {
    async fn post<T: Serialize + Sync>(
        &self,
        route: &str,
        request: &T,
    ) -> Result<(String, reqwest::Response)> {
        // Structured objects can be constructed with invalid names. Serialize
        // before building the request so validation errors remain local errors.
        let body = serde_json::to_value(request).map_err(|error| crate::Error::InvalidInput {
            message: error.to_string(),
        })?;
        let request = self
            .client
            .post(&format!("/admin/authz/{route}"))
            .json(&body);
        // Every subject selector can contain a secret. Never log these bodies,
        // and never replay an ACL mutation after an ambiguous response.
        let (id, response) = self.client.send_suppressing_body(request).await?;
        let response = self.client.check_response(&id, response).await?;
        Ok((id, response))
    }

    async fn read<T: Serialize + Sync, R: DeserializeOwned>(
        &self,
        route: &str,
        request: &T,
    ) -> Result<R> {
        let (id, response) = self.post(route, request).await?;
        response.json().await.err_to_http(id)
    }
}

#[async_trait::async_trait]
impl<S: HttpSend> Authorization for RemoteAuthorization<S> {
    async fn list_acls(
        &self,
        request: ListAccessControlEntriesRequest,
    ) -> Result<ListAccessControlEntriesResponse> {
        self.read("acl/list", &request).await
    }
    async fn add_acl(&self, request: AccessControlEntryRequest) -> Result<()> {
        self.post("acl/add", &request).await?;
        Ok(())
    }
    async fn delete_acl(&self, request: AccessControlEntryRequest) -> Result<()> {
        self.post("acl/delete", &request).await?;
        Ok(())
    }
    async fn list_role_bindings(
        &self,
        request: ListRoleBindingsRequest,
    ) -> Result<ListRoleBindingsResponse> {
        request.validate()?;
        self.read("role_binding/list", &request).await
    }
    async fn add_role_binding(&self, request: RoleBindingRequest) -> Result<()> {
        request.validate()?;
        self.post("role_binding/add", &request).await?;
        Ok(())
    }
    async fn delete_role_binding(&self, request: RoleBindingRequest) -> Result<()> {
        request.validate()?;
        self.post("role_binding/delete", &request).await?;
        Ok(())
    }
    async fn get_principal(&self, request: GetPrincipalRequest) -> Result<PrincipalInfo> {
        request.validate()?;
        self.read("principal", &request).await
    }
    async fn get_group(&self, request: GetGroupRequest) -> Result<GroupInfo> {
        request.validate()?;
        self.read("group", &request).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::remote::client::test_utils::client_with_handler;
    use serde_json::json;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    #[tokio::test]
    async fn list_preserves_wire_identity_and_continuation() {
        let authz = RemoteAuthorization {
            client: client_with_handler(|request| {
                assert_eq!(request.url().path(), "/admin/authz/acl/list");
                let body: serde_json::Value =
                    serde_json::from_slice(request.body().unwrap().as_bytes().unwrap()).unwrap();
                assert_eq!(
                    body,
                    json!({"object": "table:tenant/db:a$b$events", "subject": "G:Engineering", "max_results": 100, "continuation_token": "opaque/token"})
                );
                http::Response::builder().status(200).body(json!({
                    "entries": [{"object": "table:tenant/db:a$b$events", "subject_id": "g:team-id", "privilege": "PRIVILEGE20"}],
                    "continuation_token": "next/token"
                }).to_string()).unwrap()
            }),
        };
        let result = authz
            .list_acls(ListAccessControlEntriesRequest {
                object: Some("table:tenant/db:a$b$events".parse().unwrap()),
                subject: Some(Subject::group_name("Engineering").unwrap()),
                max_results: Some(100),
                continuation_token: Some("opaque/token".into()),
            })
            .await
            .unwrap();
        assert_eq!(result.continuation_token.as_deref(), Some("next/token"));
        assert_eq!(
            result.entries[0].subject_id,
            Subject::group_id("team-id").unwrap()
        );
        assert_eq!(result.entries[0].subject_name, None);
        assert_eq!(
            result.entries[0].privilege,
            "PRIVILEGE20".parse::<Privilege>().unwrap()
        );
    }

    #[tokio::test]
    async fn acl_failure_is_not_retried_or_reclassified() {
        for status in [404, 409, 503] {
            let calls = Arc::new(AtomicUsize::new(0));
            let count = calls.clone();
            let authz = RemoteAuthorization {
                client: client_with_handler(move |request| {
                    count.fetch_add(1, Ordering::SeqCst);
                    let body: serde_json::Value =
                        serde_json::from_slice(request.body().unwrap().as_bytes().unwrap())
                            .unwrap();
                    assert_eq!(body["privilege"], "USAGE");
                    http::Response::builder()
                        .status(status)
                        .body("failure")
                        .unwrap()
                }),
            };
            let result = authz
                .add_acl(AccessControlEntryRequest {
                    object: Object::Database(DatabaseObject::new("db".into())),
                    subject: Subject::principal_api_key("secret").unwrap(),
                    privilege: Privilege::Usage,
                })
                .await;
            let Err(crate::Error::Http {
                status_code,
                request_id,
                ..
            }) = result
            else {
                panic!("expected Http error")
            };
            assert_eq!(status_code.unwrap().as_u16(), status);
            assert!(!request_id.is_empty());
            assert_eq!(calls.load(Ordering::SeqCst), 1);
        }
    }

    #[tokio::test]
    async fn rejects_invalid_objects_before_sending() {
        let authz = RemoteAuthorization {
            client: client_with_handler(|_| -> http::Response<String> {
                panic!("invalid objects must not send a request")
            }),
        };
        let request = AccessControlEntryRequest::new(
            Object::Database(DatabaseObject::new("bad:database".into())),
            Subject::principal_api_key("secret").unwrap(),
            Privilege::Usage,
        );
        for result in [
            authz.add_acl(request.clone()).await,
            authz.delete_acl(request.clone()).await,
            authz
                .list_acls(ListAccessControlEntriesRequest {
                    object: Some(request.object),
                    ..Default::default()
                })
                .await
                .map(|_| ()),
        ] {
            assert!(matches!(result, Err(crate::Error::InvalidInput { .. })));
            assert!(!result.unwrap_err().to_string().contains("secret"));
        }
    }

    #[tokio::test]
    async fn rejects_incompatible_subjects_before_sending() {
        let authz = RemoteAuthorization {
            client: client_with_handler(|_| -> http::Response<String> {
                panic!("invalid selectors must not send a request")
            }),
        };
        let request = RoleBindingRequest {
            subject: Subject::role("reader").unwrap(),
            role: "writer".into(),
        };
        assert!(matches!(
            authz.add_role_binding(request.clone()).await,
            Err(crate::Error::InvalidInput { .. })
        ));
        assert!(matches!(
            authz.delete_role_binding(request).await,
            Err(crate::Error::InvalidInput { .. })
        ));
        let request = ListRoleBindingsRequest {
            subject: Some(Subject::role("reader").unwrap()),
            ..Default::default()
        };
        assert!(matches!(
            authz.list_role_bindings(request).await,
            Err(crate::Error::InvalidInput { .. })
        ));
        assert!(matches!(
            authz
                .get_principal(GetPrincipalRequest {
                    principal: Subject::group_id("group").unwrap()
                })
                .await,
            Err(crate::Error::InvalidInput { .. })
        ));
        assert!(matches!(
            authz
                .get_group(GetGroupRequest {
                    group: Subject::principal_api_key("secret").unwrap()
                })
                .await,
            Err(crate::Error::InvalidInput { .. })
        ));
    }
}
