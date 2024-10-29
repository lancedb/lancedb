use std::error::Error;

use napi::{Env, JsError, Status};

pub type Result<T> = napi::Result<T>;

pub trait NapiErrorExt<T> {
    /// Convert to a napi error using from_reason(err.to_string())
    fn default_error(self) -> Result<T>;
}

impl<T> NapiErrorExt<T> for std::result::Result<T, lancedb::Error> {
    fn default_error(self) -> Result<T> {
        self.map_err(|err| napi::Error::from_reason(err.to_string()))
    }
}


fn make_error_chain(env: Env, err: &lancedb::Error) -> napi::Error {
    let output = make_error(err);

    if let Some(source) = err.source() {
        let source = make_error_chain(env, source);

        // Turn into JS object so we can add the cause property
        let output = JsError::from(output);
        let output = output.into_unknown(env);
        let output = output.coerce_to_object().unwrap();
        output.set_named_property("cause", source);

        // Go back to error object
        todo!();

        output
    } else {
        output
    }
}

fn make_error(err: &lancedb::Error) -> napi::Error {
    use lancedb::Error::*;

    let err = match err {
        InvalidInput { .. } | InvalidTableName { .. } => {
            napi::Error::new(Status::InvalidArg, err.to_string())
        },
        _ => {
            napi::Error::new(Status::GenericFailure, err.to_string())
        }
    };
    err
}

fn http_from_rust_error(
    env: Env,
    err: &lancedb::Error
    request_id: &str,
    status_code: Option<u16>,
) -> JsObject {
    env.run_script(script)
}

// Use https://nodejs.org/api/errors.html#errorcause for error chain
// Set code https://nodejs.org/api/errors.html#errorcode
// Can I set it after the fact? Yes, I can.
// Maybe: https://github.com/napi-rs/napi-rs/issues/1981

// In order to use require, need this: https://github.com/napi-rs/napi-rs/issues/1522
// https://github.com/teodevgroup/teo-nodejs/blob/cfde3d846fde79221d45ebce7e1cac005e2dd4de/scripts/fixFiles.js#L126


// For converting to JSON, might need this:
// https://stackoverflow.com/questions/18391212/is-it-not-possible-to-stringify-an-error-using-json-stringify#comment112408543_18391212