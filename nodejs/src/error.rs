pub type Result<T> = napi::Result<T>;

pub trait NapiErrorExt<T> {
    /// Convert to a napi error using from_reason(err.to_string())
    fn default_error(self) -> Result<T>;
}

impl<T> NapiErrorExt<T> for std::result::Result<T, lancedb::Error> {
    fn default_error(self) -> Result<T> {
        self.map_err(|err| convert_error(&err))
    }
}

pub fn convert_error(err: &dyn std::error::Error) -> napi::Error {
    let mut message = err.to_string();

    // Append causes
    let mut cause = err.source();
    let mut indent = 2;
    while let Some(err) = cause {
        let cause_message = format!("Caused by: {}", err);
        message.push_str(&indent_string(&cause_message, indent));

        cause = err.source();
        indent += 2;
    }

    napi::Error::from_reason(message)
}

fn indent_string(s: &str, amount: usize) -> String {
    let indent = " ".repeat(amount);
    s.lines()
        .map(|line| format!("{}{}", indent, line))
        .collect::<Vec<_>>()
        .join("\n")
}
