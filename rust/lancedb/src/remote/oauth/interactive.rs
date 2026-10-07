// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use super::*;

#[derive(Debug)]
pub(super) struct ResolvedRedirect {
    pub(super) uri: String,
    pub(super) bind_addr: SocketAddr,
    pub(super) callback_path: String,
}

impl ResolvedRedirect {
    pub(super) fn new(options: &AuthorizationCodeOptions) -> Result<Self> {
        let uri = options.redirect_uri.clone().unwrap_or_else(|| {
            format!(
                "http://127.0.0.1:{}/callback",
                options.callback_port.unwrap_or(DEFAULT_CALLBACK_PORT)
            )
        });
        let parsed = Url::parse(&uri).map_err(|e| Error::InvalidInput {
            message: format!("Invalid OAuth redirect_uri: {e}"),
        })?;

        if parsed.scheme() != "http" {
            return Err(Error::InvalidInput {
                message: "OAuth redirect_uri must use http with a loopback host".to_string(),
            });
        }
        if parsed.query().is_some() || parsed.fragment().is_some() {
            return Err(Error::InvalidInput {
                message: "OAuth redirect_uri must not contain a query or fragment".to_string(),
            });
        }

        let ip = match parsed.host() {
            Some(url::Host::Domain(host)) if host.eq_ignore_ascii_case("localhost") => {
                IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)
            }
            Some(url::Host::Ipv4(ip)) if ip.is_loopback() => IpAddr::V4(ip),
            Some(url::Host::Ipv6(ip)) if ip.is_loopback() => IpAddr::V6(ip),
            Some(_) => {
                return Err(Error::InvalidInput {
                    message: "OAuth redirect_uri must use a loopback host".to_string(),
                });
            }
            None => {
                return Err(Error::InvalidInput {
                    message: "OAuth redirect_uri must include a loopback host".to_string(),
                });
            }
        };
        let port = parsed.port().ok_or(Error::InvalidInput {
            message: "OAuth redirect_uri must include a port".to_string(),
        })?;
        if port == 0 {
            return Err(Error::InvalidInput {
                message: "OAuth redirect_uri port must be greater than zero".to_string(),
            });
        }
        if let Some(callback_port) = options.callback_port
            && callback_port != port
        {
            return Err(Error::InvalidInput {
                message: format!(
                    "OAuth callback_port {callback_port} does not match redirect_uri port {port}"
                ),
            });
        }

        Ok(Self {
            uri,
            bind_addr: SocketAddr::new(ip, port),
            callback_path: parsed.path().to_string(),
        })
    }
}

#[derive(Debug)]
pub(super) struct AuthorizationRequest {
    pub(super) url: Url,
    state: String,
    pub(super) code_verifier: Option<PkceCodeVerifier>,
}

#[derive(Debug, PartialEq)]
pub(super) enum AuthorizationCallback {
    Code(String),
    ProviderError(String),
}

pub(super) struct AuthorizationCodeSource {
    pub(super) oidc: OidcClient,
    options: AuthorizationCodeOptions,
    redirect: ResolvedRedirect,
}

impl std::fmt::Debug for AuthorizationCodeSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthorizationCodeSource")
            .field("oidc", &self.oidc)
            .field("options", &self.options)
            .field("redirect", &self.redirect)
            .finish()
    }
}

impl AuthorizationCodeSource {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        issuer_url: String,
        client_id: String,
        client_secret: Option<String>,
        client_auth_method: ClientAuthMethod,
        scopes: Vec<String>,
        resource: Option<String>,
        audience: Option<String>,
        options: AuthorizationCodeOptions,
    ) -> Result<Self> {
        let redirect = ResolvedRedirect::new(&options)?;
        Ok(Self {
            oidc: OidcClient::new(
                issuer_url,
                client_id,
                client_secret,
                client_auth_method,
                scopes,
                resource,
                audience,
            )?,
            options,
            redirect,
        })
    }

    pub(super) async fn build_authorization_request(&self) -> Result<AuthorizationRequest> {
        let endpoint = self
            .oidc
            .get_discovery()
            .await?
            .authorization_endpoint
            .ok_or(Error::Runtime {
                message: "OIDC discovery did not provide authorization_endpoint".to_string(),
            })?;
        let auth_url = AuthUrl::new(endpoint).map_err(|e| Error::InvalidInput {
            message: format!("Invalid OAuth authorization_endpoint: {e}"),
        })?;
        let redirect_url =
            RedirectUrl::new(self.redirect.uri.clone()).map_err(|e| Error::InvalidInput {
                message: format!("Invalid OAuth redirect_uri: {e}"),
            })?;

        let pkce = self
            .options
            .use_pkce
            .then(PkceCodeChallenge::new_random_sha256);

        let client = self
            .oidc
            .base_client()
            .set_auth_uri(auth_url)
            .set_redirect_uri(redirect_url);
        let mut request = client.authorize_url(CsrfToken::new_random);
        for scope in &self.oidc.scopes {
            request = request.add_scope(Scope::new(scope.clone()));
        }
        for (name, value) in self.oidc.target_params() {
            request = request.add_extra_param(name, value);
        }
        if let Some((challenge, _)) = pkce.as_ref() {
            request = request.set_pkce_challenge(challenge.clone());
        }
        let (url, state) = request.url();

        Ok(AuthorizationRequest {
            url,
            state: state.secret().clone(),
            code_verifier: pkce.map(|(_, verifier)| verifier),
        })
    }

    pub(super) async fn wait_for_callback(
        &self,
        listener: &TcpListener,
        expected_state: &str,
    ) -> Result<String> {
        let deadline =
            TokioInstant::now() + Duration::from_secs(AUTHORIZATION_CALLBACK_TIMEOUT_SECS);
        loop {
            let (mut stream, _) = tokio::time::timeout_at(deadline, listener.accept())
                .await
                .map_err(|_| Error::Runtime {
                    message: "Timed out waiting for the OAuth authorization callback".to_string(),
                })?
                .map_err(|e| Error::Runtime {
                    message: format!("Failed to accept OAuth callback connection: {e}"),
                })?;
            match read_authorization_callback(
                &mut stream,
                &self.redirect.callback_path,
                expected_state,
                deadline,
            )
            .await
            {
                Ok(AuthorizationCallback::Code(code)) => {
                    write_callback_response(&mut stream, true).await;
                    return Ok(code);
                }
                Ok(AuthorizationCallback::ProviderError(message)) => {
                    write_callback_response(&mut stream, false).await;
                    return Err(Error::Runtime { message });
                }
                Err(error) => {
                    if TokioInstant::now() >= deadline {
                        return Err(Error::Runtime {
                            message: "Timed out waiting for the OAuth authorization callback"
                                .to_string(),
                        });
                    }
                    debug!("Ignoring unrelated OAuth callback connection: {error}");
                    write_callback_response(&mut stream, false).await;
                }
            }
        }
    }

    pub(super) async fn exchange_code(
        &self,
        code: &str,
        code_verifier: Option<PkceCodeVerifier>,
    ) -> Result<TokenResponse> {
        let (client, endpoint) = self.oidc.token_client().await?;
        let redirect_url =
            RedirectUrl::new(self.redirect.uri.clone()).map_err(|e| Error::InvalidInput {
                message: format!("Invalid OAuth redirect_uri: {e}"),
            })?;
        let mut request = client
            .exchange_code(oauth2::AuthorizationCode::new(code.to_string()))
            .set_redirect_uri(Cow::Owned(redirect_url));
        for (name, value) in self.oidc.target_params() {
            request = request.add_extra_param(name, value);
        }
        if let Some(verifier) = code_verifier {
            request = request.set_pkce_verifier(verifier);
        }

        request
            .request_async(&self.oidc.http_client)
            .await
            .map_err(|e| map_token_error(e, &format!("Token request to {endpoint}")))
    }
}

#[async_trait]
impl TokenSource for AuthorizationCodeSource {
    async fn fetch_token(&self) -> Result<TokenResponse> {
        let listener = TcpListener::bind(self.redirect.bind_addr)
            .await
            .map_err(|e| Error::Runtime {
                message: format!(
                    "Failed to bind OAuth callback server at {}: {e}",
                    self.redirect.bind_addr
                ),
            })?;
        let request = self.build_authorization_request().await?;
        show_oauth_prompt(&authorization_prompt(&request.url));
        launch_browser(request.url.clone());
        let code = self.wait_for_callback(&listener, &request.state).await?;
        self.exchange_code(&code, request.code_verifier).await
    }

    async fn refresh_token(&self, refresh_token: &str) -> Result<RefreshResult> {
        self.oidc.refresh_token(refresh_token).await
    }
}

/// A minimal OAuth error body, used to sniff retryable `temporarily_unavailable`
/// responses in [`OAuthHttpClient`]. Unknown fields are ignored.
#[derive(Debug, Deserialize)]
pub(super) struct OAuthErrorResponse {
    pub(super) error: String,
}

pub(super) struct DeviceCodeSource {
    oidc: OidcClient,
}

impl std::fmt::Debug for DeviceCodeSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeviceCodeSource")
            .field("oidc", &self.oidc)
            .finish()
    }
}

impl DeviceCodeSource {
    pub(super) fn new(
        issuer_url: String,
        client_id: String,
        client_secret: Option<String>,
        client_auth_method: ClientAuthMethod,
        scopes: Vec<String>,
        resource: Option<String>,
        audience: Option<String>,
    ) -> Result<Self> {
        Ok(Self {
            oidc: OidcClient::new(
                issuer_url,
                client_id,
                client_secret,
                client_auth_method,
                scopes,
                resource,
                audience,
            )?,
        })
    }

    pub(super) async fn request_device_authorization(
        &self,
    ) -> Result<StandardDeviceAuthorizationResponse> {
        let endpoint = self
            .oidc
            .get_discovery()
            .await?
            .device_authorization_endpoint
            .ok_or(Error::Runtime {
                message: "OIDC discovery did not provide device_authorization_endpoint".to_string(),
            })?;
        let device_url =
            DeviceAuthorizationUrl::new(endpoint.clone()).map_err(|e| Error::InvalidInput {
                message: format!("Invalid OAuth device_authorization_endpoint: {e}"),
            })?;

        let client = self
            .oidc
            .base_client()
            .set_device_authorization_url(device_url);
        let mut request = client.exchange_device_code();
        for scope in &self.oidc.scopes {
            request = request.add_scope(Scope::new(scope.clone()));
        }
        for (name, value) in self.oidc.target_params() {
            request = request.add_extra_param(name, value);
        }
        let device: StandardDeviceAuthorizationResponse = request
            .request_async(&self.oidc.http_client)
            .await
            .map_err(|e| {
                map_token_error(e, &format!("Device authorization request to {endpoint}"))
            })?;

        validate_oauth_url(device.verification_uri().as_str(), "verification_uri")?;
        if let Some(uri) = device.verification_uri_complete() {
            validate_oauth_url(uri.secret(), "verification_uri_complete")?;
        }
        Ok(device)
    }

    pub(super) async fn poll_for_token(
        &self,
        device: &StandardDeviceAuthorizationResponse,
    ) -> Result<TokenResponse> {
        let (client, _) = self.oidc.token_client().await?;
        let mut request = client
            .exchange_device_access_token(device)
            .set_max_backoff_interval(Duration::from_secs(10));
        for (name, value) in self.oidc.target_params() {
            request = request.add_extra_param(name, value);
        }
        request
            .request_async(
                &self.oidc.http_client,
                // RFC 8628: poll slowly; never spin faster than once a second
                // even if a misbehaving server reports a zero interval.
                |interval: Duration| tokio::time::sleep(interval.max(Duration::from_secs(1))),
                None,
            )
            .await
            .map_err(map_device_token_error)
    }
}

#[async_trait]
impl TokenSource for DeviceCodeSource {
    async fn fetch_token(&self) -> Result<TokenResponse> {
        let device = self.request_device_authorization().await?;
        show_oauth_prompt(&device_prompt(
            device.verification_uri().as_str(),
            device.user_code().secret(),
        ));
        let (browser_url, name) = device
            .verification_uri_complete()
            .map(|uri| (uri.secret().as_str(), "verification_uri_complete"))
            .unwrap_or((device.verification_uri().as_str(), "verification_uri"));
        launch_browser(validate_oauth_url(browser_url, name)?);
        self.poll_for_token(&device).await
    }

    async fn refresh_token(&self, refresh_token: &str) -> Result<RefreshResult> {
        self.oidc.refresh_token(refresh_token).await
    }
}

pub(super) fn launch_browser(url: Url) {
    drop(tokio::task::spawn_blocking(move || {
        if let Some(browser) = std::env::var_os("LANCEDB_OAUTH_BROWSER") {
            match Command::new(browser).arg(url.as_str()).status() {
                Ok(status) if !status.success() => {
                    warn!("OAuth browser helper exited with status {status}");
                }
                Err(error) => warn!("Could not run the OAuth browser helper: {error}"),
                Ok(_) => {}
            }
        } else if let Err(error) = webbrowser::open(url.as_str()) {
            warn!("Could not open an OAuth browser automatically: {error}");
        }
    }));
}

pub(super) async fn read_authorization_callback(
    stream: &mut TcpStream,
    expected_path: &str,
    expected_state: &str,
    overall_deadline: TokioInstant,
) -> Result<AuthorizationCallback> {
    const MAX_CALLBACK_REQUEST_BYTES: usize = 16 * 1024;
    let deadline = std::cmp::min(
        overall_deadline,
        TokioInstant::now() + Duration::from_secs(10),
    );
    let mut request = Vec::with_capacity(1024);
    loop {
        let mut buffer = [0; 1024];
        let count = tokio::time::timeout_at(deadline, stream.read(&mut buffer))
            .await
            .map_err(|_| Error::Runtime {
                message: "Timed out reading the OAuth authorization callback".to_string(),
            })?
            .map_err(|e| Error::Runtime {
                message: format!("Failed to read OAuth authorization callback: {e}"),
            })?;
        if count == 0 {
            return Err(Error::Runtime {
                message: "OAuth authorization callback closed before sending a request".to_string(),
            });
        }
        request.extend_from_slice(&buffer[..count]);
        if request.windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
        if request.len() >= MAX_CALLBACK_REQUEST_BYTES {
            return Err(Error::Runtime {
                message: "OAuth authorization callback request was too large".to_string(),
            });
        }
    }
    let request = std::str::from_utf8(&request).map_err(|e| Error::Runtime {
        message: format!("OAuth authorization callback was not valid UTF-8: {e}"),
    })?;
    parse_authorization_callback(request, expected_path, expected_state)
}

pub(super) fn parse_authorization_callback(
    request: &str,
    expected_path: &str,
    expected_state: &str,
) -> Result<AuthorizationCallback> {
    let request_target = request
        .lines()
        .next()
        .and_then(|line| {
            let mut parts = line.split_whitespace();
            (parts.next() == Some("GET"))
                .then(|| parts.next())
                .flatten()
        })
        .ok_or(Error::Runtime {
            message: "OAuth authorization callback was not a valid HTTP GET request".to_string(),
        })?;
    let callback =
        Url::parse(&format!("http://loopback{request_target}")).map_err(|e| Error::Runtime {
            message: format!("OAuth authorization callback URL was invalid: {e}"),
        })?;
    if callback.path() != expected_path {
        return Err(Error::Runtime {
            message: format!(
                "OAuth authorization callback used unexpected path {}",
                callback.path()
            ),
        });
    }
    let params: HashMap<_, _> = callback.query_pairs().into_owned().collect();
    if params.get("state").map(String::as_str) != Some(expected_state) {
        return Err(Error::Runtime {
            message: "OAuth authorization callback state did not match".to_string(),
        });
    }
    if let Some(error) = params.get("error") {
        let description = params
            .get("error_description")
            .map(String::as_str)
            .unwrap_or(error);
        return Ok(AuthorizationCallback::ProviderError(format!(
            "OAuth authorization failed: {description}"
        )));
    }
    params
        .get("code")
        .cloned()
        .map(AuthorizationCallback::Code)
        .ok_or(Error::Runtime {
            message: "OAuth authorization callback did not contain a code".to_string(),
        })
}

pub(super) async fn write_callback_response(stream: &mut TcpStream, success: bool) {
    let (status, body) = if success {
        (
            "200 OK",
            "<html><body><h2>Authentication successful</h2><p>You can close this window.</p></body></html>",
        )
    } else {
        (
            "400 Bad Request",
            "<html><body><h2>Authentication failed</h2><p>Return to the application for details.</p></body></html>",
        )
    };
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let _ = stream.write_all(response.as_bytes()).await;
}
