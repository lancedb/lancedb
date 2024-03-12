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
