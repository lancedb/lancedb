// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use clap::Parser;
use lance_tools::cli::LanceToolsArgs;

#[tokio::main]
pub async fn main() -> Result<(), lance_core::Error> {
    // Install global panic handler
    install_panic_handler();

    // Parse arguments from command line
    let args = LanceToolsArgs::parse();

    // Run with the parsed arguments
    return lance_result_to_std_result(args.run(&mut std::io::stdout()).await);
}

fn lance_result_to_std_result<T>(
    lance_result: lance_core::Result<T>,
) -> Result<T, lance_core::Error> {
    match lance_result {
        Ok(t) => Result::Ok(t),
        Err(e) => Result::Err(e),
    }
}

/// Install custom panic handler for better error reporting
fn install_panic_handler() {
    std::panic::set_hook(Box::new(|panic_info| {
        let msg = match panic_info.payload().downcast_ref::<&str>() {
            Some(s) => *s,
            None => match panic_info.payload().downcast_ref::<String>() {
                Some(s) => s,
                None => "Unknown panic",
            },
        };

        let location = if let Some(location) = panic_info.location() {
            format!(
                " at {}:{}:{}",
                location.file(),
                location.line(),
                location.column()
            )
        } else {
            String::new()
        };

        eprintln!("\n\x1b[31mPANIC{}: {}\x1b[0m", location, msg);

        // Print backtrace if available
        if let Ok(var) = std::env::var("RUST_BACKTRACE")
            && var != "0"
        {
            eprintln!(
                "\nBacktrace:\n{:?}",
                std::backtrace::Backtrace::force_capture()
            );
        }
    }));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ok_lance_result_to_ok_std_result() {
        assert!(lance_result_to_std_result(lance_core::Result::Ok(())).is_ok());
    }

    #[test]
    fn test_error_lance_result_to_error_std_result() {
        assert!(
            lance_result_to_std_result::<()>(lance_core::Result::Err(
                lance_core::Error::invalid_input("bad input")
            ))
            .is_err()
        );
    }
}
