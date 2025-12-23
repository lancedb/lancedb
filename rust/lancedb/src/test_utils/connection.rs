// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Functions for testing connections.

use regex::Regex;
use std::env;
use std::process::Stdio;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, ChildStdout, Command};
use tokio::sync::mpsc;

use crate::{connect, Connection};
use anyhow::{anyhow, bail, Result};
use tempfile::{tempdir, TempDir};

pub struct TestConnection {
    pub uri: String,
    pub connection: Connection,
    pub is_remote: bool,
    _temp_dir: Option<TempDir>,
    _process: Option<TestProcess>,
}

struct TestProcess {
    child: Child,
}

impl Drop for TestProcess {
    #[allow(unused_must_use)]
    fn drop(&mut self) {
        self.child.kill();
    }
}

pub async fn new_test_connection() -> Result<TestConnection> {
    match env::var("CREATE_LANCEDB_TEST_CONNECTION_SCRIPT") {
        Ok(script_path) => new_remote_connection(&script_path).await,
        Err(_e) => new_local_connection().await,
    }
}

async fn spawn_stdout_reader(
    mut stdout: BufReader<ChildStdout>,
    port_sender: mpsc::Sender<anyhow::Result<String>>,
) -> tokio::task::JoinHandle<()> {
    let print_stdout = env::var("PRINT_LANCEDB_TEST_CONNECTION_SCRIPT_OUTPUT").is_ok();
    tokio::spawn(async move {
        let mut line = String::new();
        let re = Regex::new(r"Query node now listening on 0.0.0.0:(.*)").unwrap();
        loop {
            line.clear();
            let result = stdout.read_line(&mut line).await;
            if let Err(err) = result {
                port_sender
                    .send(Err(anyhow!(
                        "error while reading from process output: {}",
                        err
                    )))
                    .await
                    .unwrap();
                return;
            } else if result.unwrap() == 0 {
                port_sender
                    .send(Err(anyhow!(
                        " hit EOF before reading port from process output."
                    )))
                    .await
                    .unwrap();
                return;
            }
            if re.is_match(&line) {
                let caps = re.captures(&line).unwrap();
                port_sender.send(Ok(caps[1].to_string())).await.unwrap();
                break;
            }
        }
        loop {
            line.clear();
            match stdout.read_line(&mut line).await {
                Err(_) => return,
                Ok(0) => return,
                Ok(_size) => {
                    if print_stdout {
                        print!("{}", line);
                    }
                }
            }
        }
    })
}

async fn new_remote_connection(script_path: &str) -> Result<TestConnection> {
    let temp_dir = tempdir()?;
    let data_path = temp_dir.path().to_str().unwrap().to_string();
    let child_result = Command::new(script_path)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .arg(data_path.clone())
        .spawn();
    if child_result.is_err() {
        bail!(format!(
            "Unable to run {}: {:?}",
            script_path,
            child_result.err()
        ));
    }
    let mut process = TestProcess {
        child: child_result.unwrap(),
    };
    let stdout = BufReader::new(process.child.stdout.take().unwrap());
    let (port_sender, mut port_receiver) = mpsc::channel(5);
    let _ = spawn_stdout_reader(stdout, port_sender).await;
    let port = match port_receiver.recv().await {
        None => bail!("Unable to determine the port number used by the phalanx process we spawned, because the reader thread was closed too soon."),
        Some(Err(err)) => bail!("Unable to determine the port number used by the phalanx process we spawned, because of an error, {}", err),
        Some(Ok(port)) => port,
    };
    let uri = "db://test";
    let host_override = format!("http://localhost:{}", port);
    let connection = create_new_connection(uri, &host_override).await?;
    Ok(TestConnection {
        uri: uri.to_string(),
        connection,
        is_remote: true,
        _temp_dir: Some(temp_dir),
        _process: Some(process),
    })
}

#[cfg(feature = "remote")]
async fn create_new_connection(uri: &str, host_override: &str) -> crate::error::Result<Connection> {
    connect(uri)
        .region("us-east-1")
        .api_key("sk_localtest")
        .host_override(host_override)
        .execute()
        .await
}

#[cfg(not(feature = "remote"))]
async fn create_new_connection(
    _uri: &str,
    _host_override: &str,
) -> crate::error::Result<Connection> {
    panic!("remote feature not supported");
}

async fn new_local_connection() -> Result<TestConnection> {
    let temp_dir = tempdir()?;
    let uri = temp_dir.path().to_str().unwrap();
    let connection = connect(uri).execute().await?;
    Ok(TestConnection {
        uri: uri.to_string(),
        connection,
        is_remote: false,
        _temp_dir: Some(temp_dir),
        _process: None,
    })
}
