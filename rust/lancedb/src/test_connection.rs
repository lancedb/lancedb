// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Functions for testing connections.

#[cfg(test)]
pub mod test_utils {
    use regex::Regex;
    use std::io::{BufRead, BufReader};
    use std::process::{Child, ChildStdout, Command, Stdio};

    use anyhow::{bail, Result};
    use tempfile::{tempdir, TempDir};
    use crate::{connect, Connection};

    const CREATE_LANCEDB_TEST_CONNECTION_SCRIPT: &str = "create_lancedb_test_connection.sh";

    pub struct TestConnection {
        pub uri: String,
        pub connection: Connection,
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
        if cfg!(feature = "test_remote_connections") {
            new_remote_connection().await
        } else {
            new_local_connection().await
        }
    }

    async fn new_remote_connection() -> Result<TestConnection> {
        let temp_dir = tempdir()?;
        let data_path = format!("{}", temp_dir.path().to_str().unwrap());
        let child_result =
            Command::new(CREATE_LANCEDB_TEST_CONNECTION_SCRIPT).
                stdin(Stdio::null()).
                stdout(Stdio::piped()).
                stderr(Stdio::piped()).
                arg(data_path.clone()).
                spawn();
        if child_result.is_err() {
            bail!(format!("Unable to run {}: {:?}",
                CREATE_LANCEDB_TEST_CONNECTION_SCRIPT, child_result.err()));
        }
        let mut process = TestProcess {
            child: child_result.unwrap(),
        };
        let stdout = BufReader::new(process.child.stdout.take().unwrap());
        let port = read_process_port(stdout)?;
        let uri = "db://test";
        let host_override = format!("http://localhost:{}", port);
        let connection = create_new_connection(&uri, &host_override).await?;
        Ok(TestConnection {
            uri: uri.to_string(),
            connection: connection,
            _temp_dir: Some(temp_dir),
            _process: Some(process),
        })
    }

    fn read_process_port(mut stdout: BufReader<ChildStdout>) -> Result<String> {
        let mut line = String::new();
        let re = Regex::new(r"Query node now listening on 0.0.0.0:(.*)").unwrap();
        loop {
            let result = stdout.read_line(&mut line);
            if result.is_err() {
                bail!(format!("read_process_port: error while reading from process output: {}",
                    result.err().unwrap()));
            }
            if re.is_match(&line) {
                let caps = re.captures(&line).unwrap();
                return Ok(caps[1].to_string());
            }
        }
    }

    #[cfg(feature = "remote")]
    async fn create_new_connection(uri: &str, host_override: &str) -> crate::error::Result<Connection> {
        connect(uri).
            region("us-east-1").
            api_key("sk_localtest").
            host_override(host_override).
            execute().await
    }

    #[cfg(not(feature = "remote"))]
    async fn create_new_connection(_uri: &str, _host_override: &str) -> crate::error::Result<Connection> {
        panic!("remote feature not supported");
    }

    async fn new_local_connection() -> Result<TestConnection> {
        let temp_dir = tempdir()?;
        let uri = temp_dir.path().to_str().unwrap();
        let connection = connect(uri).execute().await?;
        Ok(TestConnection {
            uri: uri.to_string(),
            connection: connection,
            _temp_dir: Some(temp_dir),
            _process: None,
        })
    }
}
