// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use arrow_flight::error::FlightError;
use thiserror::Error;

/// LanceDB CLI error types
#[derive(Error, Debug)]
pub enum UnifiedError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Other error: {0}")]
    Other(#[from] anyhow::Error),
}

/// Result type alias
pub type Result<T> = std::result::Result<T, UnifiedError>;

/// Convert a FlightError into a clean anyhow::Error by extracting the
/// human-readable message from tonic::Status instead of showing the full
/// Debug representation with metadata, headers, etc.
pub fn flight_error_to_anyhow(e: FlightError) -> anyhow::Error {
    match e {
        FlightError::Tonic(status) => {
            let msg = status.message();
            // Strip redundant leading "Error: " prefix that the server often adds
            let msg = msg.strip_prefix("Error: ").unwrap_or(msg);
            anyhow::anyhow!("{}", msg)
        }
        other => anyhow::anyhow!("{}", other),
    }
}

/// Convert a FlightError into an anyhow::Error tagged with the request ID the
/// statement carried.
///
/// The query node reuses a client-supplied `x-request-id` for its own logging,
/// so printing it is what lets a failed statement be found in the server log.
/// The shape matches `lancedb::Error::Http` ("(request_id=...)") so both
/// transports are greppable the same way.
pub fn flight_error_with_request_id(request_id: &str, e: FlightError) -> anyhow::Error {
    anyhow::anyhow!("(request_id={}) {}", request_id, flight_error_to_anyhow(e))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_request_id_is_prefixed_onto_the_message() {
        let status = tonic::Status::internal("Error: table 'foo' not found");
        let err = flight_error_with_request_id(
            "3f2a1b00-0000-4000-8000-000000000000",
            FlightError::Tonic(Box::new(status)),
        );
        // The whole thing has to survive a plain `{}` print: that is all the
        // REPL does with the error.
        assert_eq!(
            err.to_string(),
            "(request_id=3f2a1b00-0000-4000-8000-000000000000) table 'foo' not found"
        );
    }

    #[test]
    fn test_tonic_status_extracts_message() {
        let status = tonic::Status::internal(
            "Error: Error parsing/planning SQL: Error during planning: table 'default.public.orders' not found"
        );
        let flight_err = FlightError::Tonic(Box::new(status));
        let anyhow_err = flight_error_to_anyhow(flight_err);
        assert_eq!(
            anyhow_err.to_string(),
            "Error parsing/planning SQL: Error during planning: table 'default.public.orders' not found"
        );
    }

    #[test]
    fn test_tonic_status_without_error_prefix() {
        let status = tonic::Status::not_found("table 'foo' not found");
        let flight_err = FlightError::Tonic(Box::new(status));
        let anyhow_err = flight_error_to_anyhow(flight_err);
        assert_eq!(anyhow_err.to_string(), "table 'foo' not found");
    }

    #[test]
    fn test_non_tonic_error_passes_through() {
        let flight_err = FlightError::ProtocolError("unexpected message".to_string());
        let anyhow_err = flight_error_to_anyhow(flight_err);
        assert_eq!(anyhow_err.to_string(), "Protocol error: unexpected message");
    }
}
