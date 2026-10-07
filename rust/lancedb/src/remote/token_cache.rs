// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Persistent OAuth token cache and session lifecycle APIs.
//!
//! By default, OAuth sessions are kept in process memory only (see
//! [`OAuthHeaderProvider`](crate::remote::OAuthHeaderProvider)). Short-lived
//! processes such as CLI tools, notebooks, or scripts would otherwise have to
//! run a full interactive browser or device flow on every start. Configuring
//! [`TokenCacheOptions`] on an [`OAuthConfig`](crate::remote::OAuthConfig) opts
//! in to an explicit, hardened, on-disk cache that stores only the refresh
//! token plus non-secret metadata, so a second process can silently refresh
//! instead of re-prompting.
//!
//! Security properties:
//!
//! - Opt-in only; callers that do not configure a cache stay memory-only.
//! - Only refresh tokens are persisted. Access tokens never touch disk, so
//!   there are no local expiry decisions to get wrong when clocks move.
//! - No client secret is ever stored.
//! - The cache directory is private (`0700`) and each record is `0600`,
//!   owner-checked, and symlink-rejected on Unix; records are replaced
//!   atomically via `rename` so a crash can never leave a torn file.
//! - Cache filenames are SHA-256 hashes of the canonical issuer, client,
//!   scope, resource, audience, flow, and client-auth identity. No secret appears
//!   in a filename.
//! - Refresh-token rotation is serialized across processes with a per-key
//!   advisory file lock (`flock` on Unix, `LockFileEx` on Windows). The
//!   operating system releases these locks when a process dies, so a crash
//!   cannot leave a stale lock behind.
//!
//! Use [`OAuthSession`] to explicitly `login`, inspect `status`, or `logout`
//! without issuing a database request.
//!
//! # Example
//!
//! ```
//! use lancedb::remote::{OAuthConfig, OAuthFlow, TokenCacheOptions};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let config = OAuthConfig {
//!     issuer_url: "https://issuer.example.com".to_string(),
//!     client_id: "my-app".to_string(),
//!     client_secret: None,
//!     scopes: vec!["openid".to_string()],
//!     flow: OAuthFlow::DeviceCode,
//!     client_auth_method: None,
//!     refresh_buffer_secs: None,
//!     resource: Some("https://api.example.com".to_string()),
//!     audience: None,
//!     token_cache: Some(
//!         TokenCacheOptions::new().cache_dir("/tmp/my-app/oauth-cache"),
//!     ),
//! };
//! let session = lancedb::remote::OAuthSession::new(config)?;
//! session.login().await?;
//! let status = session.status().await?;
//! assert!(status.refreshable);
//! # Ok(())
//! # }
//! ```

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fs4::fs_std::FileExt;
use log::{debug, warn};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::error::{Error, Result};
use crate::remote::oauth::{OAuthConfig, OAuthFlow, RefreshResult, TokenResponse, TokenSource};

const CACHE_RECORD_VERSION: u32 = 1;
const DEFAULT_LOCK_TIMEOUT_SECS: u64 = 30;
const LOCK_POLL_INTERVAL: Duration = Duration::from_millis(100);

fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

/// Options for the persistent OAuth token cache.
///
/// The cache is opt-in: it is only used when set on
/// [`OAuthConfig::token_cache`](crate::remote::OAuthConfig::token_cache). See
/// the [module documentation](self) for the security properties.
#[derive(Clone, Debug, Default)]
pub struct TokenCacheOptions {
    /// Directory that holds the cached credentials.
    ///
    /// Defaults to `$XDG_CACHE_HOME/lancedb/oauth`, `$HOME/.cache/lancedb/oauth`
    /// on Unix, or `%LOCALAPPDATA%\lancedb\oauth` on Windows. The directory is
    /// created with owner-only permissions (`0700`) when missing.
    pub cache_dir: Option<PathBuf>,

    /// How long to wait for the cross-process refresh lock before failing.
    ///
    /// Defaults to 30 seconds.
    pub lock_timeout_secs: Option<u64>,
}

impl TokenCacheOptions {
    /// Create cache options with all defaults.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the directory that holds cached credentials.
    pub fn cache_dir(mut self, cache_dir: impl Into<PathBuf>) -> Self {
        self.cache_dir = Some(cache_dir.into());
        self
    }

    /// Set the cross-process refresh lock timeout in seconds.
    pub fn lock_timeout_secs(mut self, secs: u64) -> Self {
        self.lock_timeout_secs = Some(secs);
        self
    }

    fn resolved_dir(&self) -> Result<PathBuf> {
        if let Some(dir) = &self.cache_dir {
            if dir.as_os_str().is_empty() {
                return Err(Error::InvalidInput {
                    message: "OAuth token cache directory must not be empty".to_string(),
                });
            }
            return Ok(dir.clone());
        }
        default_cache_dir().ok_or_else(|| Error::InvalidInput {
            message: "Could not determine a default OAuth token cache directory; \
                      set TokenCacheOptions::cache_dir or XDG_CACHE_HOME/HOME"
                .to_string(),
        })
    }

    fn lock_timeout(&self) -> Duration {
        Duration::from_secs(self.lock_timeout_secs.unwrap_or(DEFAULT_LOCK_TIMEOUT_SECS))
    }
}

#[cfg(unix)]
fn default_cache_dir() -> Option<PathBuf> {
    let base = std::env::var_os("XDG_CACHE_HOME")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .or_else(|| {
            std::env::var_os("HOME")
                .filter(|value| !value.is_empty())
                .map(|home| {
                    let mut path = PathBuf::from(home);
                    path.push(".cache");
                    path
                })
        })?;
    let mut dir = base;
    dir.push("lancedb");
    dir.push("oauth");
    Some(dir)
}

#[cfg(windows)]
fn default_cache_dir() -> Option<PathBuf> {
    let base = std::env::var_os("LOCALAPPDATA")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)?;
    let mut dir = base;
    dir.push("lancedb");
    dir.push("oauth");
    Some(dir)
}

#[cfg(not(any(unix, windows)))]
fn default_cache_dir() -> Option<PathBuf> {
    None
}

/// A cached OAuth session record.
///
/// Only the refresh token is persisted. The metadata mirrors the cache key so
/// `status` can report what a record belongs to without exposing secrets.
#[derive(Serialize, Deserialize)]
struct CachedTokenRecord {
    version: u32,
    issuer_url: String,
    client_id: String,
    scopes: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    resource: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    audience: Option<String>,
    flow: String,
    client_auth: String,
    refresh_token: String,
    obtained_at: u64,
}

impl std::fmt::Debug for CachedTokenRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CachedTokenRecord")
            .field("version", &self.version)
            .field("issuer_url", &self.issuer_url)
            .field("client_id", &self.client_id)
            .field("scopes", &self.scopes)
            .field("resource", &self.resource)
            .field("audience", &self.audience)
            .field("flow", &self.flow)
            .field("client_auth", &self.client_auth)
            .field("refresh_token", &"<redacted>")
            .field("obtained_at", &self.obtained_at)
            .finish()
    }
}

fn canonicalize_issuer(issuer_url: &str) -> String {
    issuer_url.trim_end_matches('/').to_string()
}

fn canonicalize_scopes(scopes: &[String]) -> Vec<String> {
    let mut canonical: Vec<String> = scopes
        .iter()
        .map(|scope| scope.trim().to_string())
        .filter(|scope| !scope.is_empty())
        .collect();
    canonical.sort();
    canonical.dedup();
    canonical
}

/// Returns the cache identity of a flow, or `None` for flows that never
/// persist: client credentials have no refresh token to store, and Azure
/// managed identity is machine identity that must not enter a user token
/// cache (rejected separately with an explicit error).
fn flow_key(flow: &OAuthFlow) -> Option<&'static str> {
    match flow {
        OAuthFlow::AuthorizationCode(_) => Some("authorization_code"),
        OAuthFlow::DeviceCode => Some("device_code"),
        OAuthFlow::ClientCredentials | OAuthFlow::AzureManagedIdentity { .. } => None,
    }
}

fn client_auth_key(client_secret: Option<&str>) -> &'static str {
    if client_secret.is_some() {
        "confidential"
    } else {
        "public"
    }
}

/// Identity of one cached session: canonical issuer, client, scopes, resource,
/// audience, flow, and client-auth mode, plus the hashed filename derived from it.
#[derive(Clone, Debug)]
struct CacheKey {
    issuer_url: String,
    client_id: String,
    scopes: Vec<String>,
    resource: Option<String>,
    audience: Option<String>,
    flow: &'static str,
    client_auth: &'static str,
    file_stem: String,
}

impl CacheKey {
    fn new(config: &OAuthConfig) -> Result<Self> {
        let flow = flow_key(&config.flow).ok_or_else(|| Error::InvalidInput {
            message: format!(
                "A persistent OAuth token cache is not supported for the {:?} flow; \
                 remove TokenCacheOptions to keep tokens in memory",
                config.flow
            ),
        })?;
        let issuer_url = canonicalize_issuer(&config.issuer_url);
        let scopes = canonicalize_scopes(&config.scopes);
        let client_auth = client_auth_key(config.client_secret.as_deref());
        let identity = format!(
            "v1\n{}\n{}\n{}\n{}\n{}",
            issuer_url,
            config.client_id,
            scopes.join(" "),
            flow,
            client_auth
        );
        // Keep existing sessions reachable when no target was specified. Targeted
        // sessions use a structured encoding so parameter contents cannot collide.
        let identity = if config.resource.is_none() && config.audience.is_none() {
            identity
        } else {
            serde_json::to_string(&(
                "v2",
                &issuer_url,
                &config.client_id,
                &scopes,
                flow,
                client_auth,
                &config.resource,
                &config.audience,
            ))
            .map_err(|error| Error::Runtime {
                message: format!("Failed to encode OAuth cache identity: {error}"),
            })?
        };
        let file_stem = hex_sha256(identity.as_bytes());
        Ok(Self {
            issuer_url,
            client_id: config.client_id.clone(),
            scopes,
            resource: config.resource.clone(),
            audience: config.audience.clone(),
            flow,
            client_auth,
            file_stem,
        })
    }
}

fn hex_sha256(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut hex = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write;
        let _ = write!(hex, "{byte:02x}");
    }
    hex
}

/// Guard for the per-key cross-process refresh lock.
///
/// The lock is an advisory exclusive lock on a per-key file. The operating
/// system releases it when the owning process exits, so crashes cannot strand
/// a stale lock.
struct LockGuard {
    #[allow(dead_code)]
    file: std::fs::File,
}

/// The persistent token cache engine for one [`OAuthConfig`].
pub struct TokenCache {
    dir: PathBuf,
    key: CacheKey,
    lock_timeout: Duration,
    #[cfg(unix)]
    dir_owner: u32,
}

impl std::fmt::Debug for TokenCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TokenCache")
            .field("dir", &self.dir)
            .field("flow", &self.key.flow)
            .finish()
    }
}

impl TokenCache {
    fn new(config: &OAuthConfig, options: &TokenCacheOptions) -> Result<Self> {
        let key = CacheKey::new(config)?;
        let dir = options.resolved_dir()?;
        let lock_timeout = options.lock_timeout();
        prepare_cache_dir(&dir)?;
        #[cfg(unix)]
        let dir_owner = {
            use std::os::unix::fs::MetadataExt;
            let metadata = std::fs::metadata(&dir).map_err(|e| Error::Runtime {
                message: format!(
                    "Failed to inspect OAuth token cache directory {}: {e}",
                    dir.display()
                ),
            })?;
            metadata.uid()
        };
        Ok(Self {
            dir,
            key,
            lock_timeout,
            #[cfg(unix)]
            dir_owner,
        })
    }

    fn record_path(&self) -> PathBuf {
        let mut path = self.dir.clone();
        path.push(format!("{}.token.json", self.key.file_stem));
        path
    }

    fn lock_path(&self) -> PathBuf {
        let mut path = self.dir.clone();
        path.push(format!("{}.lock", self.key.file_stem));
        path
    }

    /// Load the cached record, if one exists and passes hardening checks.
    ///
    /// Corrupt, truncated, unknown-version, or permission-invalid records
    /// return an actionable error instead of being silently ignored or
    /// deleted; the message names the file and how to recover.
    async fn load(&self) -> Result<Option<CachedTokenRecord>> {
        let path = self.record_path();
        let dir_owner = self.dir_owner_or_zero();
        tokio::task::spawn_blocking(move || read_record(&path, dir_owner))
            .await
            .map_err(|e| Error::Runtime {
                message: format!("Failed to join OAuth token cache read: {e}"),
            })?
    }

    #[cfg(unix)]
    fn dir_owner_or_zero(&self) -> u32 {
        self.dir_owner
    }

    #[cfg(not(unix))]
    fn dir_owner_or_zero(&self) -> u32 {
        0
    }

    /// Build a record from a token response, or `None` when the response
    /// carries no refresh token (nothing may be persisted).
    fn record_from_response(&self, response: &TokenResponse) -> Option<CachedTokenRecord> {
        let refresh_token = response
            .refresh_token
            .as_ref()
            .map(|token| token.secret().clone())?;
        Some(CachedTokenRecord {
            version: CACHE_RECORD_VERSION,
            issuer_url: self.key.issuer_url.clone(),
            client_id: self.key.client_id.clone(),
            scopes: self.key.scopes.clone(),
            resource: self.key.resource.clone(),
            audience: self.key.audience.clone(),
            flow: self.key.flow.to_string(),
            client_auth: self.key.client_auth.to_string(),
            refresh_token,
            obtained_at: now_unix_secs(),
        })
    }

    /// Atomically replace the cached record.
    async fn store(&self, record: &CachedTokenRecord) -> Result<()> {
        let path = self.record_path();
        let dir = self.dir.clone();
        let payload = serde_json::to_vec(record).map_err(|e| Error::Runtime {
            message: format!("Failed to serialize OAuth token cache record: {e}"),
        })?;
        tokio::task::spawn_blocking(move || write_record(&dir, &path, &payload))
            .await
            .map_err(|e| Error::Runtime {
                message: format!("Failed to join OAuth token cache write: {e}"),
            })?
    }

    /// Delete the cached record. Returns whether a record was removed.
    async fn delete(&self) -> Result<bool> {
        let path = self.record_path();
        tokio::task::spawn_blocking(move || match std::fs::remove_file(&path) {
            Ok(()) => Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(Error::Runtime {
                message: format!(
                    "Failed to remove OAuth token cache record {}: {e}",
                    path.display()
                ),
            }),
        })
        .await
        .map_err(|e| Error::Runtime {
            message: format!("Failed to join OAuth token cache delete: {e}"),
        })?
    }

    /// Acquire the per-key cross-process lock, polling until the timeout.
    async fn acquire_lock(&self) -> Result<LockGuard> {
        let path = self.lock_path();
        let timeout = self.lock_timeout;
        tokio::task::spawn_blocking(move || {
            use std::fs::OpenOptions;
            let file = OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .truncate(false)
                .open(&path)
                .map_err(|e| Error::Runtime {
                    message: format!(
                        "Failed to open OAuth token cache lock file {}: {e}",
                        path.display()
                    ),
                })?;
            #[cfg(unix)]
            set_owner_only_permissions(&file, &path);
            let deadline = std::time::Instant::now() + timeout;
            loop {
                match file.try_lock_exclusive() {
                    Ok(true) => return Ok(LockGuard { file }),
                    Ok(false) => {
                        if std::time::Instant::now() >= deadline {
                            return Err(Error::Runtime {
                                message: format!(
                                    "Timed out after {}s waiting for the OAuth token cache \
                                     lock at {}",
                                    timeout.as_secs(),
                                    path.display()
                                ),
                            });
                        }
                        std::thread::sleep(LOCK_POLL_INTERVAL);
                    }
                    Err(error) => {
                        return Err(Error::Runtime {
                            message: format!(
                                "Failed to lock the OAuth token cache file {}: {error}",
                                path.display()
                            ),
                        });
                    }
                }
            }
        })
        .await
        .map_err(|e| Error::Runtime {
            message: format!("Failed to join OAuth token cache lock acquisition: {e}"),
        })?
    }

    /// Run the cross-process refresh critical section.
    ///
    /// Must be called with the in-process write lock held. Refresh grants are
    /// serialized by the per-key cross-process lock; interactive flows (first
    /// login or reauthentication) run outside it so a slow human-in-the-loop
    /// flow never blocks refreshes in other processes.
    pub(crate) async fn refresh_or_acquire(
        &self,
        source: &dyn TokenSource,
    ) -> Result<TokenResponse> {
        // Fast path under the lock: refresh from the durable record.
        {
            let _guard = self.acquire_lock().await?;
            // Reread the record: another process may have rotated the refresh
            // token since this process last looked.
            if let Some(record) = self.load().await? {
                match source.refresh_token(&record.refresh_token).await? {
                    RefreshResult::Refreshed(response) => {
                        self.store_if_refreshable(&response).await?;
                        return Ok(response);
                    }
                    RefreshResult::Reauthenticate => {
                        warn!(
                            "Cached OAuth refresh token was rejected; removing the cached \
                             session before reauthenticating via {:?}",
                            source
                        );
                        self.delete().await?;
                    }
                    RefreshResult::Unsupported => {}
                }
            } else {
                debug!("No cached OAuth session; acquiring one via {:?}", source);
            }
        }

        // Interactive acquisition happens without the cross-process lock.
        // Concurrent logins are independent sessions; the last store wins,
        // which is the documented multi-session rule.
        let response = source.fetch_token().await?;
        let _guard = self.acquire_lock().await?;
        self.store_if_refreshable(&response).await?;
        Ok(response)
    }

    /// Store a fresh login response (used by the eager `login` API).
    ///
    /// A successful login atomically replaces any prior session for this
    /// cache identity: when the provider does not issue a refresh token, the
    /// previous record is removed rather than left in place, so logging in
    /// can never silently keep an earlier account's credential.
    async fn store_login_response(&self, response: &TokenResponse) -> Result<()> {
        let _guard = self.acquire_lock().await?;
        match self.record_from_response(response) {
            Some(record) => self.store(&record).await?,
            None => {
                self.delete().await?;
            }
        }
        Ok(())
    }

    /// Replace the record when the response carries a refresh token.
    ///
    /// Called with the cross-process lock already held. Responses without a
    /// refresh token are not persistable and are skipped.
    async fn store_if_refreshable(&self, response: &TokenResponse) -> Result<()> {
        if let Some(record) = self.record_from_response(response) {
            self.store(&record).await?;
        } else {
            debug!(
                "OAuth response did not include a refresh token; nothing to cache for {:?}",
                self.key.flow
            );
        }
        Ok(())
    }
}

fn prepare_cache_dir(dir: &Path) -> Result<()> {
    if dir.is_dir() {
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let metadata = std::fs::metadata(dir).map_err(|e| Error::Runtime {
                message: format!(
                    "Failed to inspect OAuth token cache directory {}: {e}",
                    dir.display()
                ),
            })?;
            let mode = metadata.mode();
            if mode & 0o077 != 0 {
                return Err(Error::InvalidInput {
                    message: format!(
                        "OAuth token cache directory {} must not be accessible by group or \
                         other users (mode {:o}); run `chmod 700` on it or choose a \
                         private directory",
                        dir.display(),
                        mode & 0o777
                    ),
                });
            }
        }
        return Ok(());
    }
    if dir.exists() {
        return Err(Error::InvalidInput {
            message: format!(
                "OAuth token cache path {} exists and is not a directory",
                dir.display()
            ),
        });
    }
    let mut builder = std::fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder.create(dir).map_err(|e| Error::Runtime {
        message: format!(
            "Failed to create OAuth token cache directory {}: {e}",
            dir.display()
        ),
    })
}

fn read_record(path: &Path, dir_owner: u32) -> Result<Option<CachedTokenRecord>> {
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => {
            return Err(Error::Runtime {
                message: format!(
                    "Failed to read OAuth token cache record {}: {e}",
                    path.display()
                ),
            });
        }
    };
    if !metadata.is_file() {
        return Err(Error::InvalidInput {
            message: format!(
                "OAuth token cache record {} is not a regular file; refusing to use it. \
                 Remove the file or call logout to clear it",
                path.display()
            ),
        });
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let mode = metadata.mode();
        if mode & 0o077 != 0 {
            return Err(Error::InvalidInput {
                message: format!(
                    "OAuth token cache record {} must not be accessible by group or other \
                     users (mode {:o}); run `chmod 600` on it or call logout to clear it",
                    path.display(),
                    mode & 0o777
                ),
            });
        }
        if dir_owner != 0 && metadata.uid() != dir_owner {
            return Err(Error::InvalidInput {
                message: format!(
                    "OAuth token cache record {} is owned by a different user; refusing to \
                     use it. Remove the file or call logout to clear it",
                    path.display()
                ),
            });
        }
    }
    let payload = std::fs::read_to_string(path).map_err(|e| Error::Runtime {
        message: format!(
            "Failed to read OAuth token cache record {}: {e}",
            path.display()
        ),
    })?;
    let record: CachedTokenRecord = serde_json::from_str(&payload).map_err(|e| Error::Runtime {
        message: format!(
            "OAuth token cache record {} is corrupt ({e}); remove the file or call \
                 logout to clear it",
            path.display()
        ),
    })?;
    if record.version != CACHE_RECORD_VERSION {
        return Err(Error::Runtime {
            message: format!(
                "OAuth token cache record {} has unsupported version {}; expected {}. \
                 Remove the file or call logout to clear it",
                path.display(),
                record.version,
                CACHE_RECORD_VERSION
            ),
        });
    }
    Ok(Some(record))
}

fn write_record(dir: &Path, path: &Path, payload: &[u8]) -> Result<()> {
    let random_suffix: String = {
        use rand::Rng;
        let mut rng = rand::rng();
        (0..8)
            .map(|_| format!("{:x}", rng.random_range(0..16u32)))
            .collect()
    };
    let mut temp_path = dir.to_path_buf();
    temp_path.push(format!(
        "{}.tmp.{}.{}",
        path.file_name()
            .map(|name| name.to_string_lossy().to_string())
            .unwrap_or_default(),
        std::process::id(),
        random_suffix
    ));
    let write_attempt = || -> std::io::Result<()> {
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        let file = options.open(&temp_path)?;
        #[cfg(unix)]
        set_owner_only_permissions(&file, &temp_path);
        use std::io::Write;
        let mut writer = std::io::BufWriter::new(&file);
        writer.write_all(payload)?;
        writer.flush()?;
        drop(writer);
        file.sync_all()?;
        std::fs::rename(&temp_path, path)?;
        Ok(())
    };
    write_attempt().map_err(|e| {
        let _ = std::fs::remove_file(&temp_path);
        Error::Runtime {
            message: format!(
                "Failed to write OAuth token cache record {}: {e}",
                path.display()
            ),
        }
    })
}

#[cfg(unix)]
fn set_owner_only_permissions(file: &std::fs::File, path: &Path) {
    use std::os::unix::fs::PermissionsExt;
    if let Err(e) = file.set_permissions(std::fs::Permissions::from_mode(0o600)) {
        debug!("Could not restrict permissions on {}: {e}", path.display());
    }
}

/// Safe, non-secret view of a cached OAuth session, returned by
/// [`OAuthSession::status`] and [`OAuthSession::login`].
#[derive(Clone, Debug, PartialEq)]
pub struct SessionStatus {
    /// Whether a cached session exists that can obtain tokens without
    /// interactive authentication.
    ///
    /// Because access tokens are not persisted, this is `true` exactly when a
    /// refresh token is cached; the next connection refreshes with it rather
    /// than opening a browser or device prompt.
    pub refreshable: bool,

    /// Canonical issuer URL of the cached session.
    pub issuer_url: String,

    /// Client ID of the cached session.
    pub client_id: String,

    /// Canonical (sorted, de-duplicated) scope set of the cached session.
    pub scopes: Vec<String>,

    /// Resource indicator used to obtain the cached session, if configured.
    pub resource: Option<String>,

    /// Provider-specific audience used to obtain the cached session, if configured.
    pub audience: Option<String>,

    /// Flow that produced the cached session.
    pub flow: String,

    /// When the cached session was obtained, as Unix seconds.
    pub obtained_at: Option<u64>,
}

/// Result of [`OAuthSession::logout`].
#[derive(Clone, Debug, PartialEq)]
pub struct SessionLogout {
    /// Whether a cached credential was removed. `false` means no matching
    /// session was cached; logout is idempotent.
    pub removed: bool,
}

/// Explicit OAuth session lifecycle: eager `login`, non-secret `status`, and
/// local `logout` for the persistent token cache.
///
/// A session is built from the same [`OAuthConfig`](crate::remote::OAuthConfig)
/// used to connect (including its
/// [`token_cache`](crate::remote::OAuthConfig::token_cache) options). A
/// connection created with the same configuration shares the cache, so logging
/// in here prepares tokens for later processes without any database request.
///
/// `login` always runs the configured interactive flow and replaces the
/// cached session (the most recent login wins; see the module documentation
/// about multiple accounts). `logout` removes only the local credential; it
/// does not revoke anything with the provider and does not terminate a
/// browser SSO session.
///
/// # Example
///
/// ```
/// # use lancedb::remote::{OAuthConfig, OAuthFlow, OAuthSession, TokenCacheOptions};
/// # fn example() -> lancedb::error::Result<()> {
/// let config = OAuthConfig {
///     issuer_url: "https://issuer.example.com".to_string(),
///     client_id: "my-app".to_string(),
///     client_secret: None,
///     scopes: vec!["openid".to_string()],
///     flow: OAuthFlow::DeviceCode,
///     client_auth_method: None,
///     refresh_buffer_secs: None,
///     resource: None,
///     audience: None,
///     token_cache: Some(TokenCacheOptions::new()),
/// };
/// let session = OAuthSession::new(config)?;
/// # Ok(())
/// # }
/// ```
pub struct OAuthSession {
    token_source: Box<dyn TokenSource>,
    cache: TokenCache,
}

impl std::fmt::Debug for OAuthSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OAuthSession")
            .field("cache", &self.cache)
            .finish()
    }
}

impl OAuthSession {
    /// Create a session manager for the given configuration.
    ///
    /// The configuration must enable [`TokenCacheOptions`] and use a flow
    /// that supports persistent sessions (authorization code or device
    /// authorization).
    pub fn new(config: OAuthConfig) -> Result<Self> {
        let cache_options = config
            .token_cache
            .clone()
            .ok_or_else(|| Error::InvalidInput {
                message: "OAuthSession requires OAuthConfig.token_cache to be set".to_string(),
            })?;
        if config.scopes.is_empty() {
            return Err(Error::InvalidInput {
                message: "At least one OAuth scope is required".to_string(),
            });
        }
        let token_source = crate::remote::oauth::build_token_source(&config)?;
        let cache = TokenCache::new(&config, &cache_options)?;
        Ok(Self {
            token_source,
            cache,
        })
    }

    /// Eagerly run the configured authentication flow and store the session.
    ///
    /// Returns the resulting [`SessionStatus`]. A successful login always
    /// replaces any prior cached session for this identity; if the provider
    /// does not issue a refresh token (for example without `offline_access`),
    /// the previous record is removed and the status reports
    /// `refreshable == false`.
    pub async fn login(&self) -> Result<SessionStatus> {
        let response = self.token_source.fetch_token().await?;
        self.cache.store_login_response(&response).await?;
        self.status().await
    }

    /// Report whether a matching cached session exists, with safe metadata.
    ///
    /// This never contacts the identity provider and never exposes token
    /// values.
    pub async fn status(&self) -> Result<SessionStatus> {
        let record = self.cache.load().await?;
        Ok(match record {
            Some(record) => SessionStatus {
                refreshable: true,
                issuer_url: record.issuer_url,
                client_id: record.client_id,
                scopes: record.scopes,
                resource: record.resource,
                audience: record.audience,
                flow: record.flow,
                obtained_at: Some(record.obtained_at),
            },
            None => SessionStatus {
                refreshable: false,
                issuer_url: self.cache.key.issuer_url.clone(),
                client_id: self.cache.key.client_id.clone(),
                scopes: self.cache.key.scopes.clone(),
                resource: self.cache.key.resource.clone(),
                audience: self.cache.key.audience.clone(),
                flow: self.cache.key.flow.to_string(),
                obtained_at: None,
            },
        })
    }

    /// Remove the matching local cached credential.
    ///
    /// This only deletes the local cache entry. It does not revoke the
    /// refresh token with the provider and does not sign out of a browser
    /// SSO session. Repeated calls succeed; `removed` reports whether a
    /// credential existed.
    pub async fn logout(&self) -> Result<SessionLogout> {
        let removed = self.cache.delete().await?;
        Ok(SessionLogout { removed })
    }
}

/// Resolve the persistent cache for a configuration, if one should exist.
///
/// Returns `Ok(None)` for configurations without cache options and for the
/// client-credentials flow, which has no refresh token to persist (a debug
/// note is logged). The Azure managed-identity flow is rejected because
/// machine identity must not enter a user token cache.
pub fn token_cache_for_config(config: &OAuthConfig) -> Result<Option<Arc<TokenCache>>> {
    let Some(options) = &config.token_cache else {
        return Ok(None);
    };
    if matches!(config.flow, OAuthFlow::AzureManagedIdentity { .. }) {
        return Err(Error::InvalidInput {
            message: "A persistent OAuth token cache cannot be used with the \
                      AzureManagedIdentity flow; remove TokenCacheOptions to keep the \
                      machine identity token in memory"
                .to_string(),
        });
    }
    if matches!(config.flow, OAuthFlow::ClientCredentials) {
        debug!(
            "The client-credentials flow has no refresh token to persist; the OAuth \
             token cache is not used"
        );
        return Ok(None);
    }
    Ok(Some(Arc::new(TokenCache::new(config, options)?)))
}

#[cfg(test)]
mod tests;
