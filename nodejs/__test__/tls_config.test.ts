// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import { describe, expect, test } from "@jest/globals";
import { ClientConfig, TlsConfig } from "../lancedb";

describe("TlsConfig", () => {
  test("should create TlsConfig with all fields", () => {
    const tlsConfig: TlsConfig = {
      certFile: "/path/to/cert.pem",
      keyFile: "/path/to/key.pem",
      sslCaCert: "/path/to/ca.pem",
      assertHostname: false,
    };

    expect(tlsConfig.certFile).toBe("/path/to/cert.pem");
    expect(tlsConfig.keyFile).toBe("/path/to/key.pem");
    expect(tlsConfig.sslCaCert).toBe("/path/to/ca.pem");
    expect(tlsConfig.assertHostname).toBe(false);
  });

  test("should create TlsConfig with partial fields", () => {
    const tlsConfig: TlsConfig = {
      certFile: "/path/to/cert.pem",
      keyFile: "/path/to/key.pem",
    };

    expect(tlsConfig.certFile).toBe("/path/to/cert.pem");
    expect(tlsConfig.keyFile).toBe("/path/to/key.pem");
    expect(tlsConfig.sslCaCert).toBeUndefined();
    expect(tlsConfig.assertHostname).toBeUndefined();
  });

  test("should create ClientConfig with TlsConfig", () => {
    const tlsConfig: TlsConfig = {
      certFile: "/path/to/cert.pem",
      keyFile: "/path/to/key.pem",
      sslCaCert: "/path/to/ca.pem",
      assertHostname: true,
    };

    const clientConfig: ClientConfig = {
      userAgent: "test-agent",
      tlsConfig: tlsConfig,
    };

    expect(clientConfig.userAgent).toBe("test-agent");
    expect(clientConfig.tlsConfig).toBeDefined();
    expect(clientConfig.tlsConfig?.certFile).toBe("/path/to/cert.pem");
    expect(clientConfig.tlsConfig?.keyFile).toBe("/path/to/key.pem");
    expect(clientConfig.tlsConfig?.sslCaCert).toBe("/path/to/ca.pem");
    expect(clientConfig.tlsConfig?.assertHostname).toBe(true);
  });

  test("should handle empty TlsConfig", () => {
    const tlsConfig: TlsConfig = {};

    expect(tlsConfig.certFile).toBeUndefined();
    expect(tlsConfig.keyFile).toBeUndefined();
    expect(tlsConfig.sslCaCert).toBeUndefined();
    expect(tlsConfig.assertHostname).toBeUndefined();
  });
});
