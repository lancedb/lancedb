// Copyright 2024 Lance Developers.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/**
 * An error that occurred in the LanceDB remote client.
 * @extends Error
 * @class
 * @public
 * @category Errors
 * @param {string} message - The error message.
 * @param {string} request_id - The request ID. This can be provided in error
 *   reports to help diagnose the issue.
 * @param {number | null} status_code - The status code. May be None if th
 *   request failed before the response was received.
 * @param {any} opts - Additional options.
 */
class LanceDBClientError extends Error {
    request_id: string;
    status_code: number | null;

    constructor(message: string, request_id: string, status_code: number | null, opts: any) {
        super(message, opts);
        this.name = "LanceDBClientError";
        this.request_id = request_id;
        this.status_code = status_code;
    }
}

/**
 * An error that occurred in the LanceDB remote client.
 * @extends LanceDBClientError
 */
class HttpError extends LanceDBClientError {}

/**
 * An error that occurred in the LanceDB remote client.
 * 
 * @extends LanceDBClientError
 * 
 * @help The retry strategy can be adjusted by passing {@link lancedb.RetryConfig}
 * to {@link lancedb.connect}.
 * 
 * @category Errors
 */
class RetryError extends LanceDBClientError {
    request_failures: number;
    connect_failures: number;
    read_failures: number;
    max_request_failures: number;
    max_connect_failures: number;
    max_read_failures: number;

    constructor(message: string, request_id: string, status_code: number, opts: any) {
        super(message, request_id, status_code, { cause: opts.cause });
        this.name = "RetryError";
        this.request_failures = opts.request_failures;
        this.connect_failures = opts.connect_failures;
        this.read_failures = opts.read_failures;
        this.max_request_failures = opts.max_request_failures;
        this.max_connect_failures = opts.max_connect_failures;
        this.max_read_failures = opts.max_read_failures;
    }
}
