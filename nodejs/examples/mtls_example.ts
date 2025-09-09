// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

/**
 * Example of using mTLS configuration with LanceDB remote connection
 */

import { connect, ClientConfig, TlsConfig } from "@lancedb/lancedb";

async function connectWithMTLS() {
  // Configure mTLS settings
  const tlsConfig: TlsConfig = {
    // Path to client certificate for mTLS authentication
    certFile: "/path/to/client.crt",
    // Path to client private key for mTLS authentication
    keyFile: "/path/to/client.key",
    // Path to CA certificate for server verification
    sslCaCert: "/path/to/ca.crt",
    // Whether to verify hostname (set to true in production)
    assertHostname: false,
  };

  // Create client configuration with TLS settings
  const clientConfig: ClientConfig = {
    userAgent: "MyApp/1.0",
    tlsConfig: tlsConfig,
    // Optional: Configure timeouts
    timeoutConfig: {
      timeout: 30,        // Overall timeout in seconds
      connectTimeout: 10, // Connection timeout in seconds
      readTimeout: 20,    // Read timeout in seconds
    },
    // Optional: Configure retries
    retryConfig: {
      retries: 3,
      connectRetries: 3,
      backoffFactor: 0.5,
    },
  };

  // Connect to remote LanceDB with mTLS
  const connection = await connect({
    uri: "db://mydb",
    apiKey: process.env.LANCEDB_API_KEY || "",
    region: "us-east-1",
    clientConfig: clientConfig,
  });

  // Use the connection...
  const tableNames = await connection.listTables();
  console.log("Tables:", tableNames);

  return connection;
}

// Example with only CA certificate (one-way TLS)
async function connectWithCACert() {
  const tlsConfig: TlsConfig = {
    // Only provide CA cert for server verification
    sslCaCert: "/path/to/ca.crt",
    assertHostname: true,
  };

  const clientConfig: ClientConfig = {
    tlsConfig: tlsConfig,
  };

  const connection = await connect({
    uri: "db://mydb",
    apiKey: process.env.LANCEDB_API_KEY || "",
    region: "us-east-1",
    clientConfig: clientConfig,
  });

  return connection;
}

// Example with environment-based configuration
async function connectWithEnvConfig() {
  const tlsConfig: TlsConfig = {
    certFile: process.env.CLIENT_CERT_FILE,
    keyFile: process.env.CLIENT_KEY_FILE,
    sslCaCert: process.env.CA_CERT_FILE,
    assertHostname: process.env.NODE_ENV === "production",
  };

  // Only add TLS config if certificates are provided
  const clientConfig: ClientConfig = {};
  if (tlsConfig.certFile || tlsConfig.sslCaCert) {
    clientConfig.tlsConfig = tlsConfig;
  }

  const connection = await connect({
    uri: process.env.LANCEDB_URI || "db://mydb",
    apiKey: process.env.LANCEDB_API_KEY || "",
    region: process.env.LANCEDB_REGION || "us-east-1",
    clientConfig: clientConfig,
  });

  return connection;
}

// Run the example
if (require.main === module) {
  connectWithEnvConfig()
    .then((conn) => {
      console.log("Successfully connected with mTLS");
      return conn.disconnect();
    })
    .catch((error) => {
      console.error("Connection failed:", error);
      process.exit(1);
    });
}

export { connectWithMTLS, connectWithCACert, connectWithEnvConfig };